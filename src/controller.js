const { executeQuery } = require("./db/connection");

const fetchAndSaveData = async () => {
  const BATCH_SIZE = Math.min(process.env.BATCH_SIZE, 30000); // Respect server limit
  const API_KEY = process.env.DUNE_API_KEY;
  const QUERY_ID = process.env.DUNE_QUERY_ID;
  let totalProcessed = 0;

  try {
    console.log("Starting data fetch with pagination...");
    let hasMoreData = true;
    let offset = 0;

    while (hasMoreData) {
      const queryParams = new URLSearchParams({
        limit: BATCH_SIZE,
        offset: offset,
        allow_partial_results: "true", // Always allow partial results
      });

      const url = `https://api.dune.com/api/v1/query/${QUERY_ID}/results?${queryParams}`;

      const response = await fetch(url, {
        method: "GET",
        headers: { "X-DUNE-API-KEY": API_KEY },
      });

      const data = await response.json();

      if (!data.result || !data.result.rows || data.result.rows.length === 0) {
        hasMoreData = false;
        continue;
      }

      const rows = data.result.rows;
      const totalRows = data.result.metadata.total_row_count;

      // Create batch insert query
      const valueStrings = rows
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

      const params = rows.flatMap((row) => [
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

      await executeQuery(query, params);

      totalProcessed += rows.length;
      const progressPercent = ((totalProcessed / totalRows) * 100).toFixed(2);

      // Clear line and update progress
      process.stdout.write(
        `\rProgress: ${progressPercent}% (${totalProcessed.toLocaleString()}/${totalRows.toLocaleString()} rows)`
      );

      offset += rows.length; // Use actual rows returned instead of BATCH_SIZE
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    console.log("\nData import completed successfully!");
    console.log(
      `Final count: ${totalProcessed.toLocaleString()} rows processed`
    );
  } catch (error) {
    console.error("\nError fetching and saving data:", error);
    throw error;
  }
};

module.exports = {
  fetchAndSaveData,
};
