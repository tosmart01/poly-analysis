import "antd/dist/reset.css";
import { ConfigProvider } from "antd";
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import "./styles.css";

function readServerDefaults() {
  const node = document.getElementById("server-defaults");
  if (!node) {
    return {};
  }
  try {
    return JSON.parse(node.textContent || "{}");
  } catch (_error) {
    return {};
  }
}

const rootNode = document.getElementById("root");
if (!rootNode) {
  throw new Error("missing #root container");
}
const serverDefaults = readServerDefaults();

createRoot(rootNode).render(
  <React.StrictMode>
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: "#178f98",
          borderRadius: 14,
          colorBgContainer: "rgba(255,255,255,0.62)",
          colorBorder: "rgba(111,126,151,0.22)",
          colorText: "#182334",
          colorTextSecondary: "#607089",
          fontFamily: "'Plus Jakarta Sans', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        },
        components: {
          Card: {
            colorBgContainer: "rgba(255,255,255,0.54)",
            headerBg: "transparent",
          },
          Table: {
            headerBg: "rgba(255,255,255,0.48)",
            rowHoverBg: "rgba(255,255,255,0.5)",
          },
          Modal: {
            contentBg: "rgba(255,255,255,0.78)",
            headerBg: "transparent",
          },
        },
      }}
    >
      <App serverDefaults={serverDefaults} />
    </ConfigProvider>
  </React.StrictMode>,
);
