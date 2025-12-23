import App from './app.js';

/**
 * entry point for the modular application.
 * waits for the DOM to be ready before initializing the App bootstrap.
 */
document.addEventListener('DOMContentLoaded', () => {
  console.log('[Main] DOMContentLoaded fired. Starting App.init()...');
  App.init().then(() => {
    console.log('[Main] App.init() completed successfully.');
  }).catch(err => {
    console.error('[Main] Critical bootstrap failure:', err);
    document.body.innerHTML = `<div style="color:red; padding:20px;"><h1>Critical Error</h1><pre>${err.stack}</pre></div>`;
  });
});
