const { executeQuery } = require("./db/connection");

const fetchAndSaveData = async () => {
  const API_BATCH_SIZE = Math.min(process.env.BATCH_SIZE || 30000, 30000); // API fetch size
  const DB_BATCH_SIZE = 1000; // Database insert size
  const API_KEY = process.env.DUNE_API_KEY;
  const QUERY_ID = process.env.DUNE_QUERY_ID;
  let totalProcessed = 0;

  try {
    console.log("Starting data fetch with pagination...");
    let hasMoreData = true;
    let offset = 0;
    totalProcessed = offset;
    const queryParams = new URLSearchParams({
      limit: API_BATCH_SIZE,
      offset: offset,
      allow_partial_results: 'true'
    });
    
    let url = `https://api.dune.com/api/v1/query/${QUERY_ID}/results?${queryParams}`;

    while (hasMoreData) {      
      const response = await fetch(url, {
        method: "GET",
        headers: { "X-DUNE-API-KEY": API_KEY },
      });

      const data = await response.json();

      if (!data.result || !data.result.rows || data.result.rows.length === 0) {
        console.log("\n", data);
        if (!data.error) {
          hasMoreData = false;
        }
        continue;
      }

      url = data.next_uri;

      const rows = data.result.rows;
      const totalRows = data.result.metadata.total_row_count;

      // Process the API batch in smaller chunks for DB insertion
      for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
        const chunk = rows.slice(i, i + DB_BATCH_SIZE);
        
        // Validate and prepare parameters
        const params = [];
        const valuePlaceholders = [];
        
        chunk.forEach((row, idx) => {
          const rowParams = [
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
          ];

          if (rowParams.every(param => param !== undefined)) {
            params.push(...rowParams);
            const offset = idx * 12;
            valuePlaceholders.push(
              `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12})`
            );
          }
        });

        if (params.length > 0) {
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
            ) VALUES ${valuePlaceholders.join(', ')}
            ON CONFLICT (swap_transaction_id) DO NOTHING
          `;

          await executeQuery(query, params);
        }

        totalProcessed += chunk.length;
        const progressPercent = ((totalProcessed / totalRows) * 100).toFixed(2);
        
        process.stdout.write(
          `\rProgress: ${progressPercent}% (${totalProcessed.toLocaleString()}/${totalRows.toLocaleString()} rows)`
        );
      }

      offset += rows.length;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    console.log('\nData import completed successfully!');
    console.log(`Final count: ${totalProcessed.toLocaleString()} rows processed`);
  } catch (error) {
    console.error("\nError fetching and saving data:", error);
    throw error;
  }
};

module.exports = {
  fetchAndSaveData,
};