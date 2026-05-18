const STORAGE_KEY = "analysis-poly.address-book.v1";
const DEFAULT_ENTRY_NAME = "Default Address";

export function normalizeAddress(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

export function isAddressLike(value) {
  return normalizeAddress(value).startsWith("0x");
}

export function shortAddress(value) {
  const address = normalizeAddress(value);
  if (address.length <= 14) {
    return address;
  }
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

export function sortAddressBook(entries) {
  return [...entries].sort((left, right) => {
    if (left.isDefault !== right.isDefault) {
      return left.isDefault ? -1 : 1;
    }
    return left.name.localeCompare(right.name);
  });
}

export function sanitizeAddressBook(entries) {
  const deduped = [];
  const seen = new Set();

  for (const item of Array.isArray(entries) ? entries : []) {
    const address = normalizeAddress(item?.address);
    if (!isAddressLike(address) || seen.has(address)) {
      continue;
    }
    seen.add(address);
    deduped.push({
      id: address,
      name: String(item?.name || "").trim() || shortAddress(address),
      address,
      isDefault: Boolean(item?.isDefault),
    });
  }

  if (!deduped.length) {
    return [];
  }

  if (!deduped.some((item) => item.isDefault)) {
    deduped[0] = { ...deduped[0], isDefault: true };
  } else {
    let assigned = false;
    for (let index = 0; index < deduped.length; index += 1) {
      const isDefault = deduped[index].isDefault && !assigned;
      deduped[index] = { ...deduped[index], isDefault };
      if (isDefault) {
        assigned = true;
      }
    }
  }

  return sortAddressBook(deduped);
}

export function getDefaultAddressEntry(entries) {
  const sanitized = sanitizeAddressBook(entries);
  return sanitized.find((item) => item.isDefault) || sanitized[0] || null;
}

export function loadAddressBook(seedAddress = "") {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    const saved = sanitizeAddressBook(parsed);
    if (saved.length) {
      return saved;
    }
  } catch (_error) {}

  const fallbackAddress = normalizeAddress(seedAddress);
  if (!isAddressLike(fallbackAddress)) {
    return [];
  }

  return [
    {
      id: fallbackAddress,
      name: DEFAULT_ENTRY_NAME,
      address: fallbackAddress,
      isDefault: true,
    },
  ];
}

export function persistAddressBook(entries) {
  if (typeof window === "undefined") {
    return;
  }

  const sanitized = sanitizeAddressBook(entries).map(({ id, name, address, isDefault }) => ({
    id,
    name,
    address,
    isDefault,
  }));
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(sanitized));
}
