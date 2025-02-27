/**
 * Logging utilities for Cloudflare Agents
 * 
 * This module provides structured logging utilities to help debug and monitor
 * agent behavior in development and production environments.
 */

/**
 * Log levels for agent logging
 */
export enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARN = 'warn',
  ERROR = 'error'
}

/**
 * Configuration options for the logger
 */
export interface LoggerOptions {
  /**
   * Minimum log level to display (defaults to INFO in production, DEBUG in development)
   */
  minLevel?: LogLevel;
  
  /**
   * Whether to include timestamps in logs (default: true)
   */
  timestamps?: boolean;
  
  /**
   * Whether to enable pretty printing in development (default: true)
   */
  pretty?: boolean;
  
  /**
   * Optional custom log destination function
   * If not provided, logs to console
   */
  destination?: (logEntry: LogEntry) => void;
  
  /**
   * Additional context values to include with all logs
   */
  defaultContext?: Record<string, any>;
}

/**
 * A structured log entry
 */
export interface LogEntry {
  /**
   * Log level
   */
  level: LogLevel;
  
  /**
   * Log message
   */
  message: string;
  
  /**
   * Timestamp in ISO format
   */
  timestamp: string;
  
  /**
   * Agent ID if applicable
   */
  agentId?: string;
  
  /**
   * Connection ID if applicable
   */
  connectionId?: string;
  
  /**
   * Additional contextual information
   */
  context?: Record<string, any>;
  
  /**
   * Error information if present
   */
  error?: {
    message: string;
    stack?: string;
    name?: string;
  };
}

/**
 * A logger instance for agents
 */
export class AgentLogger {
  private options: Required<LoggerOptions>;
  private agentId?: string;
  
  /**
   * Create a new logger instance
   * 
   * @param options Logger configuration options
   * @param agentId Optional agent ID to associate with all logs
   */
  constructor(options: LoggerOptions = {}, agentId?: string) {
    // Default options
    this.options = {
      minLevel: options.minLevel ?? (
        typeof process !== 'undefined' && process.env?.NODE_ENV === 'production' 
          ? LogLevel.INFO 
          : LogLevel.DEBUG
      ),
      timestamps: options.timestamps ?? true,
      pretty: options.pretty ?? true,
      destination: options.destination ?? this.defaultDestination.bind(this),
      defaultContext: options.defaultContext ?? {}
    };
    
    this.agentId = agentId;
  }
  
  /**
   * Log a message at DEBUG level
   */
  debug(message: string, context?: Record<string, any>): void {
    this.log(LogLevel.DEBUG, message, context);
  }
  
  /**
   * Log a message at INFO level
   */
  info(message: string, context?: Record<string, any>): void {
    this.log(LogLevel.INFO, message, context);
  }
  
  /**
   * Log a message at WARN level
   */
  warn(message: string, context?: Record<string, any>): void {
    this.log(LogLevel.WARN, message, context);
  }
  
  /**
   * Log a message at ERROR level
   */
  error(message: string, error?: Error, context?: Record<string, any>): void {
    const errorDetails = error ? {
      message: error.message,
      stack: error.stack,
      name: error.name
    } : undefined;
    
    this.log(LogLevel.ERROR, message, context, errorDetails);
  }
  
  /**
   * Create a child logger with additional context
   */
  child(additionalContext: Record<string, any>, connectionId?: string): AgentLogger {
    const childLogger = new AgentLogger({
      ...this.options,
      defaultContext: {
        ...this.options.defaultContext,
        ...additionalContext
      }
    }, this.agentId);
    
    if (connectionId) {
      childLogger.setConnectionId(connectionId);
    }
    
    return childLogger;
  }
  
  /**
   * Set the agent ID for this logger
   */
  setAgentId(agentId: string): void {
    this.agentId = agentId;
  }
  
  /**
   * Set a connection ID for connection-specific logs
   */
  setConnectionId(connectionId: string): void {
    this.options.defaultContext.connectionId = connectionId;
  }
  
  /**
   * Log a message with the given level
   */
  private log(
    level: LogLevel, 
    message: string, 
    context?: Record<string, any>,
    error?: LogEntry['error']
  ): void {
    // Skip if below minimum log level
    if (!this.shouldLog(level)) {
      return;
    }
    
    const logEntry: LogEntry = {
      level,
      message,
      timestamp: new Date().toISOString(),
      context: {
        ...this.options.defaultContext,
        ...context
      },
      agentId: this.agentId,
      connectionId: this.options.defaultContext.connectionId,
    };
    
    if (error) {
      logEntry.error = error;
    }
    
    this.options.destination(logEntry);
  }
  
  /**
   * Default log destination writing to console
   */
  private defaultDestination(logEntry: LogEntry): void {
    const { level, message, timestamp, context, error, agentId, connectionId } = logEntry;
    
    // Format based on log level
    let logFn: (...args: any[]) => void;
    
    switch (level) {
      case LogLevel.DEBUG:
        logFn = console.debug;
        break;
      case LogLevel.INFO:
        logFn = console.info;
        break;
      case LogLevel.WARN:
        logFn = console.warn;
        break;
      case LogLevel.ERROR:
        logFn = console.error;
        break;
      default:
        logFn = console.log;
    }
    
    // Create prefix for the log message
    let prefix = '';
    if (this.options.timestamps) {
      prefix += `[${timestamp}] `;
    }
    
    prefix += `[${level.toUpperCase()}]`;
    
    if (agentId) {
      prefix += ` [Agent:${agentId}]`;
    }
    
    if (connectionId) {
      prefix += ` [Conn:${connectionId}]`;
    }
    
    if (this.options.pretty) {
      // Pretty print in development
      logFn(`${prefix} ${message}`);
      
      if (Object.keys(context || {}).length > 0) {
        console.log('Context:', context);
      }
      
      if (error) {
        console.log('Error:', error);
        if (error.stack) {
          console.log(error.stack);
        }
      }
    } else {
      // Structured log for production/parsing
      logFn(JSON.stringify(logEntry));
    }
  }
  
  /**
   * Determine if a message with this level should be logged
   */
  private shouldLog(level: LogLevel): boolean {
    const levels = [LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR];
    const configuredLevelIndex = levels.indexOf(this.options.minLevel);
    const currentLevelIndex = levels.indexOf(level);
    
    return currentLevelIndex >= configuredLevelIndex;
  }
}

/**
 * Create a default logger instance
 */
export function createLogger(options?: LoggerOptions, agentId?: string): AgentLogger {
  return new AgentLogger(options, agentId);
}

/**
 * Global default logger instance
 */
export const defaultLogger = createLogger(); 