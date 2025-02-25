const { executeQuery } = require("./db/connection");

const fetchAndSaveData = async () => {
  const BATCH_SIZE = process.env.BATCH_SIZE;
  const API_KEY = process.env.DUNE_API_KEY;
  const QUERY_ID = process.env.DUNE_QUERY_ID;
  let totalProcessed = 0;

  try {
    console.log("Starting data fetch with pagination...");
    let hasMoreData = true;
    let offset = 0;

    // First, get total count
    const countOptions = {
      method: "GET",
      headers: {
        "X-DUNE-API-KEY": API_KEY,
      },
    };
    const countUrl = `https://api.dune.com/api/v1/query/${QUERY_ID}/results?limit=1`;
    const countResponse = await fetch(countUrl, countOptions);
    const countData = await countResponse.json();
    const totalRows = countData.result.metadata.total_row_count;

    console.log(`Total rows to process: ${totalRows.toLocaleString()}`);

    while (hasMoreData) {
      const options = {
        method: "GET",
        headers: {
          "X-DUNE-API-KEY": API_KEY,
        },
      };

      const queryParams = new URLSearchParams({
        limit: BATCH_SIZE,
        offset: offset,
      });

      const url = `https://api.dune.com/api/v1/query/${QUERY_ID}/results?${queryParams}`;

      const response = await fetch(url, options);
      const data = await response.json();

      if (!data.result || !data.result.rows || data.result.rows.length === 0) {
        hasMoreData = false;
        continue;
      }

      // Create batch insert query
      const valueStrings = data.result.rows
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

      const params = data.result.rows.flatMap((row) => [
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

      totalProcessed += data.result.rows.length;
      const progressPercent = ((totalProcessed / totalRows) * 100).toFixed(2);

      // Clear line and update progress
      process.stdout.write(
        `\rProgress: ${progressPercent}% (${totalProcessed.toLocaleString()}/${totalRows.toLocaleString()} rows)`
      );

      offset += parseInt(BATCH_SIZE);
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
