const additions = Number(process.env.PR_ADDITIONS || 0);
const deletions = Number(process.env.PR_DELETIONS || 0);
const changedFiles = Number(process.env.PR_CHANGED_FILES || 0);

const totalChanges = additions + deletions;
const maxChanges = Number(process.env.MAX_PR_CHANGES || 1500);
const maxFiles = Number(process.env.MAX_PR_FILES || 40);

if (totalChanges > maxChanges || changedFiles > maxFiles) {
  console.error(
    `PR too large: changes=${totalChanges}/${maxChanges}, files=${changedFiles}/${maxFiles}`
  );
  process.exit(1);
}

console.log(
  `PR size check passed: changes=${totalChanges}/${maxChanges}, files=${changedFiles}/${maxFiles}`
);

