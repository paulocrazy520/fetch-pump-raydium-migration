require("dotenv").config();
const pkg = require("pg");
const { Pool } = pkg;

let client = null;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20, // increase connection pool
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 60000,
  statement_timeout: 60000, // 1 minute
});

pool.on("connect", () => {
  console.log("Connected to the database");
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle client", err);
  process.exit(-1);
});

const executeQuery = async (query, params) => {
  try {
    client = client || (await pool.connect());
    const result = await client.query(query, params);
    return result.rows;
  } catch (error) {
    console.error("Database query error:", error);
    throw error;
  }
};

const initializeDB = async () => {
  try {
    const createPumpDataTableQuery = `
      CREATE TABLE IF NOT EXISTS pump_data (
          id SERIAL PRIMARY KEY,
          block_slot BIGINT,
          block_time TIMESTAMP(3) WITH TIME ZONE,
          token_address VARCHAR,
          transaction_id VARCHAR,
          swap_block_slot BIGINT,
          swap_block_time TIMESTAMP(3) WITH TIME ZONE,
          swap_transaction_id VARCHAR UNIQUE,  -- Added UNIQUE constraint
          swap_trader_id VARCHAR,
          sol_amount DECIMAL(38, 18),
          token_amount DECIMAL(38, 18),
          token_price DECIMAL(38, 18),
          top10_percent DOUBLE PRECISION
      );

      -- Create indexes for common queries
      CREATE INDEX IF NOT EXISTS pump_data_token_address_index ON pump_data USING btree (token_address);
      CREATE INDEX IF NOT EXISTS pump_data_block_time_index ON pump_data USING btree (block_time);
      CREATE INDEX IF NOT EXISTS pump_data_swap_block_time_index ON pump_data USING btree (swap_block_time);

      -- Create unique index for swap_transaction_id
      CREATE UNIQUE INDEX IF NOT EXISTS pump_data_swap_transaction_id_unique ON pump_data (swap_transaction_id);
    `;

    await executeQuery(createPumpDataTableQuery);
  } catch (error) {
    console.error("Database initialization error:", error);
    throw error;
  }
};

module.exports = {
  executeQuery,
  initializeDB,
};
