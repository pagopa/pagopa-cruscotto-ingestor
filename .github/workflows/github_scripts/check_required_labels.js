const requiredLabels = ["feat", "fix", "chore", "refactor", "docs", "test"];

function fail(message) {
  console.error(message);
  process.exit(1);
}

const raw = process.env.PR_LABELS || "";
const labels = raw
  .split(",")
  .map((v) => v.trim().toLowerCase())
  .filter(Boolean);

if (labels.length === 0) {
  fail(`PR has no labels. Add one of: ${requiredLabels.join(", ")}`);
}

const hasRequired = labels.some((l) => requiredLabels.includes(l));
if (!hasRequired) {
  fail(`Missing required label. Add one of: ${requiredLabels.join(", ")}`);
}

console.log("Required label check passed.");

