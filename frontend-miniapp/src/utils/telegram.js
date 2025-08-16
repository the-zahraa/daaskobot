export function initTelegramWebApp() {
  if (!window.Telegram) return null;
  const tg = window.Telegram.WebApp;
  try { tg.expand(); } catch(e) {}
  return tg;
}

export function sendDataToBot(data) {
  const tg = window.Telegram?.WebApp;
  if (!tg) {
    console.warn("Telegram WebApp not available.");
    return;
  }
  tg.sendData(JSON.stringify(data));
}
