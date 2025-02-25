const { fetchAndSaveData } = require("./controller");
const { initializeDB } = require("./db/connection");

const initialize = async () => {
  try {
    // Initialize the database
    initializeDB();

    // Start fetching
    await fetchAndSaveData();

    // Add process handlers for clean shutdown
    process.on("SIGTERM", cleanup);
    process.on("SIGINT", cleanup);
  } catch (error) {
    console.error("Initialization error:", error);
    process.exit(1);
  }
};

const cleanup = async () => {
  try {
    console.log("\nShutting down...");
    // Add any cleanup code here
    process.exit(0);
  } catch (error) {
    console.error("Error during cleanup:", error);
    process.exit(1);
  }
};

// Start the application
initialize();
