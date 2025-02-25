module.exports = {
  apps: [
    {
      name: "fetch-pump-raydium-migration",
      script: "yarn start",
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: "development",
      },
      env_production: {
        NODE_ENV: "production",
      },
    },
  ],
};
