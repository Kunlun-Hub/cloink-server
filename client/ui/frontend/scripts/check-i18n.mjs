import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const localeRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../i18n/locales");

function readLocale(code) {
    const file = path.join(localeRoot, code, "common.json");
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    const messages = new Map();

    for (const [key, entry] of Object.entries(parsed)) {
        if (!entry || typeof entry.message !== "string") {
            throw new Error(`${code}/${path.basename(file)} has an invalid message: ${key}`);
        }
        if (messages.has(key)) throw new Error(`${code} has duplicate key: ${key}`);
        messages.set(key, entry.message);
    }

    return messages;
}

const en = readLocale("en");
const zhCN = readLocale("zh-CN");
const placeholderPattern = /\{([A-Za-z][A-Za-z0-9_]*)\}/g;
const placeholders = (value) =>
    [...value.matchAll(placeholderPattern)].map((match) => match[1]).sort();
const errors = [];

for (const [key, enValue] of en) {
    if (!enValue.trim()) errors.push(`en has empty value: ${key}`);
    if (!zhCN.has(key)) {
        errors.push(`zh-CN missing key: ${key}`);
        continue;
    }

    const zhValue = zhCN.get(key);
    if (!zhValue.trim()) errors.push(`zh-CN has empty value: ${key}`);
    if (placeholders(enValue).join(",") !== placeholders(zhValue).join(",")) {
        errors.push(`placeholder mismatch: ${key}`);
    }
}

for (const key of zhCN.keys()) {
    if (!en.has(key)) errors.push(`zh-CN has extra key: ${key}`);
}

if (errors.length > 0) {
    console.error(errors.join("\n"));
    process.exit(1);
}

console.log(`i18n check passed: ${en.size} keys in en and zh-CN`);
