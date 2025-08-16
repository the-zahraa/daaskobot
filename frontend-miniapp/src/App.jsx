import { useEffect, useState } from "react";

const Card = ({ title, price, period, features, cta, onClick, secondary }) => (
  <div className="rounded-2xl shadow p-6 border" style={{ maxWidth: 380 }}>
    <h3 style={{ fontSize: 22, margin: 0 }}>{title}</h3>
    <div style={{ fontSize: 28, fontWeight: 700, marginTop: 8 }}>{price}</div>
    <div style={{ color: "#666", marginBottom: 16 }}>{period}</div>
    <ul style={{ margin: 0, paddingLeft: 18 }}>
      {features.map((f, i) => <li key={i} style={{ marginBottom: 6 }}>{f}</li>)}
    </ul>
    <button
      onClick={onClick}
      style={{
        marginTop: 16,
        width: "100%",
        padding: "10px 14px",
        borderRadius: 12,
        border: "none",
        fontWeight: 600,
        cursor: "pointer",
        background: secondary ? "#eee" : "#0ea5e9",
        color: secondary ? "#111" : "white",
      }}
    >
      {cta}
    </button>
  </div>
);

export default function App() {
  const [tg, setTg] = useState(null);

  useEffect(() => {
    const tgw = window.Telegram?.WebApp;
    if (tgw) {
      tgw.expand();
      tgw.ready();
      setTg(tgw);
    }
  }, []);

  const openInChatPay = () => {
    // Deep link back to bot to trigger /buy_pro
    const botUsername = import.meta.env.VITE_BOT_USERNAME || "";
    if (!botUsername) {
      alert("Bot username missing. Set VITE_BOT_USERNAME in .env.");
      return;
    }
    const url = `https://t.me/${botUsername}?start=BUY_PRO`;
    if (tg?.openTelegramLink) tg.openTelegramLink(url);
    else window.open(url, "_blank");
  };

  return (
    <div style={{ padding: 20, display: "grid", gap: 16 }}>
      <h1 style={{ margin: 0, fontSize: 26 }}>Subscriptions</h1>
      <div style={{ color: "#555", marginBottom: 10 }}>
        Pay safely with <b>Telegram Stars</b> inside the chat. No external providers.
      </div>
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
        <Card
          title="Free"
          price="0★"
          period="forever"
          features={[
            "Basic join tracking",
            "Force-join gate",
            "Tenant dashboard",
          ]}
          cta="Current Plan"
          onClick={() => {}}
          secondary
        />
        <Card
          title="Pro"
          price="1000★"
          period="30 days"
          features={[
            "Advanced analytics & reports",
            "Mass messaging (rate-limited)",
            "Invite link campaigns",
            "Priority support",
          ]}
          cta="Pay in Chat"
          onClick={openInChatPay}
        />
      </div>
    </div>
  );
}
