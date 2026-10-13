// Configuración pm2 para peak-worker.
// Las variables (credenciales, modo) se cargan desde .env vía dotenv (ver index.js), no acá.
module.exports = {
  apps: [
    {
      name:               'peak-worker',
      script:             'index.js',
      cwd:                __dirname,
      instances:          1,
      exec_mode:          'fork',
      autorestart:        true,
      max_restarts:       10,
      restart_delay:      5000,
      watch:              false,
      max_memory_restart: '200M',
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
