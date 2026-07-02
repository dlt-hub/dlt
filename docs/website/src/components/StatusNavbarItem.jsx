import React, { useEffect, useState } from "react";

const COLORS = {
  none: "#28a745",
  minor: "#f4b400",
  major: "#f57c00",
  critical: "#d93025",
};

export default function StatusNavbarItem() {
  const [indicator, setIndicator] = useState("none");

  useEffect(() => {
    let cancelled = false;

    const fetchStatus = () => {
      fetch("https://status.dlthub.com/api/v2/status.json")
        .then((r) => r.json())
        .then((d) => {
          if (!cancelled) setIndicator(d?.status?.indicator || "none");
        })
        .catch(() => {});
    };

    fetchStatus();
    const interval = setInterval(fetchStatus, 60000);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  return (
    <a
      href="https://status.dlthub.com"
      target="_blank"
      rel="noopener noreferrer"
      className="navbar__item navbar__link"
      aria-label="dltHub platform status"
    >
      <span
        aria-hidden="true"
        style={{
          display: "inline-block",
          width: "8px",
          height: "8px",
          borderRadius: "50%",
          background: COLORS[indicator] || "#8a8a8a",
          marginRight: "6px",
          verticalAlign: "middle",
        }}
      />
      Status
    </a>
  );
}
