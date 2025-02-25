const { DuneClient } = require("@duneanalytics/client-sdk");
const { executeQuery } = require("./db/connection");

const dune = new DuneClient("gHLx5IHcZeEigW6qjmLXiajEMFdd4CYh");

const BATCH_SIZE = 1000; // Adjust based on your memory constraints

const fetchAndSaveData = async () => {
  try {
    console.log("Fetching data from Dune...");
    const query_result = await dune.getLatestResult({ queryId: 4772663 });
    const rows = query_result.result.rows;
    const totalRows = rows.length;

    console.log(`Total rows to process: ${totalRows.toLocaleString()}`);

    // Process in batches
    for (let i = 0; i < totalRows; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);

      // Create parameterized query for the batch
      const valueStrings = batch
        .map(
          (_, index) =>
            `($${index * 12 + 1}, $${index * 12 + 2}, $${index * 12 + 3}, $${
              index * 12 + 4
            }, $${index * 12 + 5}, $${index * 12 + 6}, $${index * 12 + 7}, $${
              index * 12 + 8
            }, $${index * 12 + 9}, $${index * 12 + 10}, $${index * 12 + 11}, $${
              index * 12 + 12
            })`
        )
        .join(", ");

      const query = `
        INSERT INTO pump_data (
          block_slot,
          block_time,
          token_address,
          transaction_id,
          swap_block_slot,
          swap_block_time,
          swap_transaction_id,
          swap_trader_id,
          sol_amount,
          token_amount,
          token_price,
          top10_percent
        ) VALUES ${valueStrings}
        ON CONFLICT (swap_transaction_id) DO NOTHING
      `;

      // Flatten the batch into a single array of parameters
      const params = batch.flatMap((row) => [
        row.block_slot,
        row.block_time,
        row.token_address,
        row.transaction_id,
        row.swap_block_slot,
        row.swap_block_time,
        row.swap_transaction_id,
        row.swap_trader_id,
        row.sol_amount,
        row.token_amount,
        row.token_price,
        row.top10_percent,
      ]);

      // Execute batch insert
      await executeQuery(query, params);

      // Log progress
      const progress = Math.round(((i + batch.length) / totalRows) * 100);
      const processedRows = (i + batch.length).toLocaleString();
      process.stdout.write(
        `Progress: ${progress}% (${processedRows}/${totalRows.toLocaleString()} rows)\r`
      );

      // Optional: Add small delay to prevent overwhelming the database
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    console.log("\nCompleted saving all records to database");
  } catch (error) {
    console.error("Error fetching or saving data:", error);
    throw error;
  }
};

module.exports = {
  fetchAndSaveData,
};
