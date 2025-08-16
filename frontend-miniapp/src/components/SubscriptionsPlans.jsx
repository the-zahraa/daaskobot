import React from "react";

const plans = [
  { code: "PRO_MONTH", title: "Pro — 30 days", desc: "Advanced analytics, campaigns & mass DM.", stars: 300 },
  { code: "PRO_QUARTER", title: "Pro — 90 days", desc: "3 months at a discount.", stars: 800 },
];

export default function SubscriptionPlans() {
  const botUsername = import.meta.env.VITE_BOT_USERNAME;

  const openInChat = (code) => {
    if (!botUsername) {
      alert("VITE_BOT_USERNAME is not set. Add it to frontend-miniapp/.env");
      return;
    }
    const url = `https://t.me/${botUsername}?start=BUY_PRO_${code}`;
    window.open(url, "_blank");
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-3xl mx-auto p-4">
        <h1 className="text-2xl font-bold mb-4">⭐ Choose a plan</h1>
        <div className="grid sm:grid-cols-2 gap-4">
          {plans.map((p) => (
            <div key={p.code} className="rounded-2xl shadow p-5 bg-white">
              <div className="text-lg font-semibold">{p.title}</div>
              <div className="text-gray-600 mt-1">{p.desc}</div>
              <div className="text-3xl font-extrabold mt-4">{p.stars}★</div>
              <button
                onClick={() => openInChat(p.code)}
                className="mt-4 w-full rounded-xl px-4 py-2 bg-black text-white hover:opacity-90"
              >
                Pay in Telegram (Stars)
              </button>
            </div>
          ))}
        </div>
        <p className="mt-6 text-sm text-gray-500">
          Checkout happens in Telegram chat. After payment, your subscription activates instantly.
        </p>
      </div>
    </div>
  );
}
