import sql from "mssql";
import { Tool } from "@modelcontextprotocol/sdk/types.js";

export class CreateTableTool implements Tool {
  [key: string]: any;
  name = "create_table";
  description = "Creates a new table in the MSSQL Database with the specified columns.";
  inputSchema = {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table to create" },
      columns: {
        type: "array",
        description: "Array of column definitions (e.g., [{ name: 'id', type: 'INT PRIMARY KEY' }, ...])",
        items: {
          type: "object",
          properties: {
            name: { type: "string", description: "Column name" },
            type: { type: "string", description: "SQL type and constraints (e.g., 'INT PRIMARY KEY', 'NVARCHAR(255) NOT NULL')" }
          },
          required: ["name", "type"]
        }
      }
    },
    required: ["tableName", "columns"],
  } as any;

  async run(params: any) {
    try {
      const { tableName, columns } = params;
      if (!tableName || typeof tableName !== 'string') {
        throw new Error("'tableName' must be a non-empty string");
      }
      if (!Array.isArray(columns) || columns.length === 0) {
        throw new Error("'columns' must be a non-empty array");
      }

      // Basic validation for identifiers (schema & table & column names).
      const identRegex = /^[A-Za-z_][A-Za-z0-9_]*$/;

      // Support schema-qualified names: schema.table. Allow bracketed already but normalize.
      const parts = tableName
        .replace(/\]/g, '') // remove stray closing brackets to simplify split
        .split('.')
        .map(p => p.replace(/\[/g, '').trim())
        .filter(p => p.length > 0);

      let schemaName: string | null = null;
      let pureTableName: string;
      if (parts.length === 2) {
        [schemaName, pureTableName] = parts;
      } else {
        pureTableName = parts[0];
      }

      if (!identRegex.test(pureTableName)) {
        throw new Error(`Invalid table name '${pureTableName}'. Use alphanumerics and underscores, starting with a letter or underscore.`);
      }
      if (schemaName && !identRegex.test(schemaName)) {
        throw new Error(`Invalid schema name '${schemaName}'.`);
      }

      const request = new sql.Request();

      // Create schema if specified and missing
      if (schemaName) {
        const createSchemaIfMissing = `IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'${schemaName}') EXEC('CREATE SCHEMA [${schemaName}]');`;
        await request.batch(createSchemaIfMissing);
      }

      // Build column definitions with validation on column names
      const columnDefs = columns.map((col: any) => {
        if (!col?.name || !identRegex.test(col.name)) {
          throw new Error(`Invalid column name '${col?.name}'.`);
        }
        if (!col?.type || typeof col.type !== 'string') {
          throw new Error(`Column '${col.name}' is missing a valid type string.`);
        }
        return `[${col.name}] ${col.type}`;
      }).join(', ');

      const qualifiedName = schemaName ? `[${schemaName}].[${pureTableName}]` : `[${pureTableName}]`;
      const createTableQuery = `CREATE TABLE ${qualifiedName} (${columnDefs})`;
      await request.query(createTableQuery);

      return {
        success: true,
        message: `Table '${qualifiedName}' created successfully.`,
        schemaCreated: !!schemaName
      };
    } catch (error) {
      console.error("Error creating table:", error);
      return {
        success: false,
        message: `Failed to create table: ${error}`
      };
    }
  }
}
