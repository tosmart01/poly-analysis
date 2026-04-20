export const DEFAULT_KEYWORDS = "";

export const COLOR_PALETTE = ["#2fa7b6", "#3f8ddb", "#d184b1", "#7a63ee", "#2ca472", "#e0914a"];

export const EMPTY_SUMMARY = {
  total_realized_pnl_usdc: 0,
  total_taker_fee_usdc: 0,
  total_maker_reward_usdc: 0,
};

export function buildDefaultForm(serverDefaults) {
  return {
    address: serverDefaults.default_address || "",
    keywords: serverDefaults.default_keywords || DEFAULT_KEYWORDS,
    startTime: "",
    endTime: "",
    feeRateBps: "1000",
    missingCostWarnQty: "0.5",
    concurrency: "5",
    pageLimit: "1000",
  };
}
