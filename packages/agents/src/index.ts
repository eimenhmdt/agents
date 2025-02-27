import {
  Server,
  routePartykitRequest,
  type PartyServerOptions,
  getServerByName,
  type Connection,
  type ConnectionContext,
  type WSMessage,
} from "partyserver";

import { parseCronExpression } from "cron-schedule";
import { nanoid } from "nanoid";

export type { Connection, WSMessage, ConnectionContext } from "partyserver";

import { WorkflowEntrypoint as CFWorkflowEntrypoint } from "cloudflare:workers";
import { AgentLogger, LogLevel, createLogger, type LoggerOptions } from "./logging";

// Re-export logging utilities
export { AgentLogger, LogLevel, createLogger, type LoggerOptions } from "./logging";

/**
 * A class for creating workflow entry points that can be used with Cloudflare Workers
 */
export class WorkflowEntrypoint extends CFWorkflowEntrypoint {}

/**
 * Represents a scheduled task within an Agent
 * @template T Type of the payload data
 */
export type Schedule<T = string> = {
  /** Unique identifier for the schedule */
  id: string;
  /** Name of the method to be called */
  callback: string;
  /** Data to be passed to the callback */
  payload: T;
} & (
  | {
      /** Type of schedule for one-time execution at a specific time */
      type: "scheduled";
      /** Timestamp when the task should execute */
      time: number;
    }
  | {
      /** Type of schedule for delayed execution */
      type: "delayed";
      /** Timestamp when the task should execute */
      time: number;
      /** Number of seconds to delay execution */
      delayInSeconds: number;
    }
  | {
      /** Type of schedule for recurring execution based on cron expression */
      type: "cron";
      /** Timestamp for the next execution */
      time: number;
      /** Cron expression defining the schedule */
      cron: string;
    }
);

function getNextCronTime(cron: string) {
  const interval = parseCronExpression(cron);
  return interval.getNextDate();
}

const STATE_ROW_ID = "cf_state_row_id";
const STATE_WAS_CHANGED = "cf_state_was_changed";

const DEFAULT_STATE = {} as unknown;

/**
 * Base class for creating Agent implementations
 * @template Env Environment type containing bindings
 * @template State State type to store within the Agent
 */
export class Agent<Env, State = unknown> extends Server<Env> {
  #state = DEFAULT_STATE as State;
  
  /**
   * Logger instance for this agent
   */
  protected logger: AgentLogger;
  
  /**
   * Initial state for the Agent
   * Override to provide default state values
   */
  initialState: State = DEFAULT_STATE as State;

  /**
   * Get the current state of the Agent
   */
  get state(): State {
    if (this.#state !== DEFAULT_STATE) {
      // state was previously set, and populated internal state
      return this.#state;
    } else {
      // looks like this is the first time the state is being accessed
      // check if the state was set in a previous life
      const wasChanged = this.sql<{ state: "true" | undefined }>`
        SELECT state FROM cf_agents_state WHERE id = ${STATE_WAS_CHANGED}
      `;

      // ok, let's pick up the actual state from the db
      const result = this.sql<{ state: State | undefined }>`
      SELECT state FROM cf_agents_state WHERE id = ${STATE_ROW_ID}
    `;

      if (
        wasChanged[0]?.state === "true" ||
        // we do this check for people who updated their code before we shipped wasChanged
        result[0]?.state
      ) {
        const state = result[0]?.state as string; // could be null?

        this.#state = JSON.parse(state);
        return this.#state;
      }

      // ok, this is the first time the state is being accessed
      // and the state was not set in a previous life
      // so we need to set the initial state (if provided)
      if (this.initialState === DEFAULT_STATE) {
        // no initial state provided, so we return undefined
        return undefined as State;
      } else {
        // initial state provided, so we set the state,
        // update db and return the initial state
        this.setState(this.initialState);
        return this.initialState;
      }
    }
  }

  /**
   * Agent configuration options
   */
  static options = {
    /** Whether the Agent should hibernate when inactive */
    hibernate: true, // default to hibernate
  };
  
  /**
   * Execute SQL queries against the Agent's database
   * @template T Type of the returned rows
   * @param strings SQL query template strings
   * @param values Values to be inserted into the query
   * @returns Array of query results
   */
  sql<T = Record<string, any>>(
    strings: TemplateStringsArray,
    ...values: any[]
  ) {
    let query = "";
    try {
      // Construct the SQL query with placeholders
      query = strings.reduce(
        (acc, str, i) => acc + str + (i < values.length ? "?" : ""),
        ""
      );

      // Execute the SQL query with the provided values
      return [...this.ctx.storage.sql.exec(query, ...values)] as T[];
    } catch (e) {
      console.error(`failed to execute sql query: ${query}`, e);
      throw e;
    }
  }

  /**
   * Constructor for the Agent class
   * @param ctx Agent context from Durable Objects
   * @param env Environment containing bindings
   */
  constructor(ctx: AgentContext, env: Env) {
    super(ctx, env);
    
    // Initialize logger with agent ID
    this.logger = createLogger({
      defaultContext: { agent: this.constructor.name }
    }, (ctx.id as any)?.toString());
    
    this.logger.debug('Agent instantiated');
    
    this.sql`
      CREATE TABLE IF NOT EXISTS cf_agents_state (
        id TEXT PRIMARY KEY NOT NULL,
        state TEXT
      )
    `;

    void this.ctx.blockConcurrencyWhile(async () => {
      try {
        // Create alarms table if it doesn't exist
        this.sql`
        CREATE TABLE IF NOT EXISTS cf_agents_schedules (
          id TEXT PRIMARY KEY NOT NULL DEFAULT (randomblob(9)),
          callback TEXT,
          payload TEXT,
          type TEXT NOT NULL CHECK(type IN ('scheduled', 'delayed', 'cron')),
          time INTEGER,
          delayInSeconds INTEGER,
          cron TEXT,
          created_at INTEGER DEFAULT (unixepoch())
        )
      `;

        // execute any pending alarms and schedule the next alarm
        await this.alarm();
      } catch (e) {
        console.error(e);
        throw e;
      }
    });

    const _onMessage = this.onMessage.bind(this);
    this.onMessage = async (connection: Connection, message: WSMessage): Promise<void> => {
      if (
        typeof message === "string" &&
        message.startsWith("cf_agent_state:")
      ) {
        const parsed = JSON.parse(message.slice(15));
        this.#setStateInternal(parsed.state, connection);
        return;
      }
      return _onMessage(connection, message);
    };

    const _onConnect = this.onConnect.bind(this);
    this.onConnect = (connection: Connection, ctx: ConnectionContext) => {
      // TODO: This is a hack to ensure the state is sent after the connection is established
      // must fix this
      setTimeout(() => {
        if (this.state) {
          connection.send(
            `cf_agent_state:` +
              JSON.stringify({ type: "cf_agent_state", state: this.state })
          );
        }
        _onConnect(connection, ctx);
      }, 20);
    };
  }

  #setStateInternal(state: State, source: Connection | "server" = "server") {
    this.#state = state;
    this.sql`
    INSERT OR REPLACE INTO cf_agents_state (id, state)
    VALUES (${STATE_ROW_ID}, ${JSON.stringify(state)})
  `;
    this.sql`
    INSERT OR REPLACE INTO cf_agents_state (id, state)
    VALUES (${STATE_WAS_CHANGED}, ${JSON.stringify(true)})
  `;
    this.broadcast(
      `cf_agent_state:` +
        JSON.stringify({
          type: "cf_agent_state",
          state: state,
        }),
      source !== "server" ? [source.id] : []
    );
    this.onStateUpdate(state, source);
  }

  /**
   * Update the Agent's state and persist it
   * @param state New state to set
   */
  setState(state: State) {
    this.logger.debug('Setting state', { 
      previousStateKeys: Object.keys(this.#state || {}),
      newStateKeys: Object.keys(state || {})
    });
    
    this.#state = state;
    this.sql`
    INSERT OR REPLACE INTO cf_agents_state (id, state)
    VALUES (${STATE_ROW_ID}, ${JSON.stringify(state)})
  `;
    this.sql`
    INSERT OR REPLACE INTO cf_agents_state (id, state)
    VALUES (${STATE_WAS_CHANGED}, ${JSON.stringify(true)})
  `;
    this.broadcast(
      `cf_agent_state:` +
        JSON.stringify({
          type: "cf_agent_state",
          state: state,
        }),
      []
    );
    this.onStateUpdate(state, "server");
  }

  /**
   * Called when the Agent's state is updated
   * @param state Updated state
   * @param source Source of the update
   */
  onStateUpdate(state: State | undefined, source: Connection | "server") {
    // Override in subclass if needed
    this.logger.debug('State updated', { 
      source: typeof source === 'string' ? source : 'connection',
      stateKeys: Object.keys(state || {})
    });
  }

  /**
   * Handle incoming email messages for this Agent
   * @param email The email message received
   */
  onEmail(email: ForwardableEmailMessage) {
    this.logger.info('Email received', {
      from: email.from,
      to: email.to,
      subject: email.headers.get('subject')
    });
    // Override in subclass if needed
  }
  
  /**
   * Render content (not implemented in base class)
   */
  render() {
    throw new Error("Not implemented");
  }

  /**
   * Schedule a task to be executed in the future
   * @param when When to execute the task (Date, seconds from now, or cron expression)
   * @param callback Name of the method to call
   * @param payload Data to pass to the method
   * @returns Information about the scheduled task
   */
  async schedule<T = string>(
    when: Date | string | number,
    callback: keyof this,
    payload?: T
  ): Promise<Schedule<T>> {
    this.logger.info('Scheduling task', { 
      callback: callback as string,
      when: typeof when === 'object' ? when.toISOString() : when
    });
    
    const id = nanoid(9);

    if (typeof callback !== "string") {
      throw new Error("Callback must be a string");
    }

    if (typeof this[callback] !== "function") {
      throw new Error(`this.${callback} is not a function`);
    }

    if (when instanceof Date) {
      const timestamp = Math.floor(when.getTime() / 1000);
      this.sql`
        INSERT OR REPLACE INTO cf_agents_schedules (id, callback, payload, type, time)
        VALUES (${id}, ${callback}, ${JSON.stringify(
        payload
      )}, 'scheduled', ${timestamp})
      `;

      await this.scheduleNextAlarm();

      return {
        id,
        callback: callback,
        payload: payload as T,
        time: timestamp,
        type: "scheduled",
      };
    } else if (typeof when === "number") {
      const time = new Date(Date.now() + when * 1000);
      const timestamp = Math.floor(time.getTime() / 1000);

      this.sql`
        INSERT OR REPLACE INTO cf_agents_schedules (id, callback, payload, type, delayInSeconds, time)
        VALUES (${id}, ${callback}, ${JSON.stringify(
        payload
      )}, 'delayed', ${when}, ${timestamp})
      `;

      await this.scheduleNextAlarm();

      return {
        id,
        callback: callback,
        payload: payload as T,
        delayInSeconds: when,
        time: timestamp,
        type: "delayed",
      };
    } else if (typeof when === "string") {
      const nextExecutionTime = getNextCronTime(when);
      const timestamp = Math.floor(nextExecutionTime.getTime() / 1000);

      this.sql`
        INSERT OR REPLACE INTO cf_agents_schedules (id, callback, payload, type, cron, time)
        VALUES (${id}, ${callback}, ${JSON.stringify(
        payload
      )}, 'cron', ${when}, ${timestamp})
      `;

      await this.scheduleNextAlarm();

      return {
        id,
        callback: callback,
        payload: payload as T,
        cron: when,
        time: timestamp,
        type: "cron",
      };
    } else {
      throw new Error("Invalid schedule type");
    }
  }
  
  /**
   * Get a specific scheduled task by ID
   * @param id ID of the scheduled task
   * @returns The schedule information or undefined if not found
   */
  async getSchedule<T = string>(id: string): Promise<Schedule<T> | undefined> {
    this.logger.debug('Getting schedule by ID', { scheduleId: id });
    const result = this.sql<Schedule<string>>`
      SELECT * FROM cf_agents_schedules WHERE id = ${id}
    `;
    if (!result) return undefined;

    return { ...result[0], payload: JSON.parse(result[0].payload) as T };
  }
  
  /**
   * Get all scheduled tasks, optionally filtered by criteria
   * @param criteria Optional filtering criteria
   * @returns Array of matching scheduled tasks
   */
  getSchedules<T = string>(
    criteria: {
      description?: string;
      id?: string;
      type?: "scheduled" | "delayed" | "cron";
      timeRange?: { start?: Date; end?: Date };
    } = {}
  ): Schedule<T>[] {
    this.logger.debug('Getting schedules', { criteria });
    let query = "SELECT * FROM cf_agents_schedules WHERE 1=1";
    const params = [];

    if (criteria.id) {
      query += " AND id = ?";
      params.push(criteria.id);
    }

    if (criteria.description) {
      query += " AND description = ?";
      params.push(criteria.description);
    }

    if (criteria.type) {
      query += " AND type = ?";
      params.push(criteria.type);
    }

    if (criteria.timeRange) {
      query += " AND time >= ? AND time <= ?";
      const start = criteria.timeRange.start || new Date(0);
      const end = criteria.timeRange.end || new Date(999999999999999);
      params.push(
        Math.floor(start.getTime() / 1000),
        Math.floor(end.getTime() / 1000)
      );
    }

    const result = this.ctx.storage.sql
      .exec(query, ...params)
      .toArray()
      .map((row) => ({
        ...row,
        payload: JSON.parse(row.payload as string) as T,
      })) as Schedule<T>[];

    return result;
  }

  /**
   * Cancel a scheduled task
   * @param id ID of the task to cancel
   * @returns Whether the task was successfully cancelled
   */
  async cancelSchedule(id: string): Promise<boolean> {
    this.logger.info('Cancelling schedule', { scheduleId: id });
    this.sql`DELETE FROM cf_agents_schedules WHERE id = ${id}`;

    await this.scheduleNextAlarm();
    return true;
  }

  private async scheduleNextAlarm() {
    // Find the next schedule that needs to be executed
    const result = this.sql`
      SELECT time FROM cf_agents_schedules 
      WHERE time > ${Math.floor(Date.now() / 1000)}
      ORDER BY time ASC 
      LIMIT 1
    `;
    if (!result) return;

    if (result.length > 0 && "time" in result[0]) {
      const nextTime = result[0].time * 1000;
      await this.ctx.storage.setAlarm(nextTime);
    }
  }

  /**
   * Called when the alarm fires to execute scheduled tasks
   */
  async alarm() {
    this.logger.debug('Alarm triggered');
    const now = Math.floor(Date.now() / 1000);

    // Get all schedules that should be executed now
    const result = this.sql<Schedule<string>>`
      SELECT * FROM cf_agents_schedules WHERE time <= ${now}
    `;

    for (const row of result || []) {
      const callback = this[row.callback as keyof Agent<Env>];
      if (!callback) {
        console.error(`callback ${row.callback} not found`);
        continue;
      }
      try {
        (
          callback as (
            payload: unknown,
            schedule: Schedule<unknown>
          ) => Promise<void>
        ).bind(this)(JSON.parse(row.payload as string), row);
      } catch (e) {
        console.error(`error executing callback ${row.callback}`, e);
      }
      if (row.type === "cron") {
        // Update next execution time for cron schedules
        const nextExecutionTime = getNextCronTime(row.cron);
        const nextTimestamp = Math.floor(nextExecutionTime.getTime() / 1000);

        this.sql`
          UPDATE cf_agents_schedules SET time = ${nextTimestamp} WHERE id = ${row.id}
        `;
      } else {
        // Delete one-time schedules after execution
        this.sql`
          DELETE FROM cf_agents_schedules WHERE id = ${row.id}
        `;
      }
    }

    // Schedule the next alarm
    await this.scheduleNextAlarm();
  }

  /**
   * Handle WebSocket connections to this Agent
   * @param connection The established WebSocket connection
   * @param ctx Additional context for the connection
   */
  onConnect(connection: Connection, ctx: ConnectionContext): void | Promise<void> {
    const connectionLogger = this.logger.child({ connectionId: connection.id });
    connectionLogger.debug('New connection established');
    
    // Send initial state if it exists
    if (this.#state && Object.keys(this.#state).length > 0) {
      connectionLogger.debug('Sending initial state to new connection');
      connection.send(
        "cf_agent_state:" + JSON.stringify({ type: "cf_agent_state", state: this.#state })
      );
    }
  }

  /**
   * Handle WebSocket messages sent to this Agent
   * @param connection The WebSocket connection that sent the message
   * @param message The message data
   */
  async onMessage(connection: Connection, message: WSMessage): Promise<void> {
    const connectionLogger = this.logger.child({ connectionId: connection.id });
    
    try {
      if (typeof message === "string" && message.startsWith("cf_agent_state:")) {
        const parsedMessage = JSON.parse(message.slice(15));
        
        if (parsedMessage?.type === "cf_agent_state" && parsedMessage?.state) {
          connectionLogger.debug('Received state update from client');
          this.#state = parsedMessage.state as State;
          this.onStateUpdate(this.#state, connection);
          this.broadcast(
            "cf_agent_state:" + JSON.stringify({ type: "cf_agent_state", state: this.#state }),
            [connection.id]
          );
        }
      } else {
        connectionLogger.debug('Received message from client', { 
          messageType: typeof message,
          messageLength: typeof message === 'string' ? message.length : undefined
        });
      }
    } catch (error) {
      connectionLogger.error('Error processing message', error as Error, {
        messageType: typeof message
      });
    }
  }

  /**
   * Handle WebSocket close events
   * @param connection The WebSocket connection that closed
   */
  onClose(connection: Connection): void | Promise<void> {
    this.logger.debug('Connection closed', { connectionId: connection.id });
  }

  /**
   * Clean up resources before the Agent is destroyed
   */
  async destroy() {
    this.logger.info('Agent being destroyed');
    // drop all tables
    this.sql`DROP TABLE IF EXISTS cf_agents_state`;
    this.sql`DROP TABLE IF EXISTS cf_agents_schedules`;

    // delete all alarms
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
  }
}

/**
 * Namespace for creating Agent instances
 * @template Agentic Type of the Agent class
 */
export type AgentNamespace<Agentic extends Agent<unknown>> =
  DurableObjectNamespace<Agentic>;

/**
 * Agent's durable context
 */
export type AgentContext = DurableObjectState;

/**
 * Configuration options for Agent routing
 */
export type AgentOptions<Env> = PartyServerOptions<Env>;

/**
 * Route a request to the appropriate Agent
 * @param request Request to route
 * @param env Environment containing Agent bindings
 * @param options Routing options
 * @returns Response from the Agent or undefined if no route matched
 */
export function routeAgentRequest<Env>(
  request: Request,
  env: Env,
  options?: AgentOptions<Env>
) {
  return routePartykitRequest(request, env as Record<string, unknown>, {
    prefix: "agents",
    ...(options as PartyServerOptions<Record<string, unknown>>),
  });
}

/**
 * Route an email to the appropriate Agent
 * @param email Email message to route
 * @param env Environment containing Agent bindings
 * @param options Routing options
 */
export async function routeAgentEmail<Env>(
  email: ForwardableEmailMessage,
  env: Env,
  options?: AgentOptions<Env>
): Promise<void> {}

/**
 * Get or create an Agent by name
 * @template Env Environment type containing bindings
 * @template T Type of the Agent class
 * @param namespace Agent namespace
 * @param name Name of the Agent instance
 * @param options Options for Agent creation
 * @returns Promise resolving to an Agent instance stub
 */
export function getAgentByName<Env, T extends Agent<Env>>(
  namespace: AgentNamespace<T>,
  name: string,
  options?: {
    jurisdiction?: DurableObjectJurisdiction;
    locationHint?: DurableObjectLocationHint;
  }
) {
  return getServerByName<Env, T>(namespace, name, options);
}