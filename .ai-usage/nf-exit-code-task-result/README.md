### AI usage: [nf-exit-code-task-result] Decide task success from the exit code, not the Fovus status

1 Claude Code session on `nf-exit-code-task-result` (PR #89), 2026-11-14 22:42 to 2026-11-17 16:44 UTC. Tokens come from the session logs and their subagents; cost uses list prices (price table 2026-10). Active time adds up gaps of 5 minutes or less between model calls. Outside reviewers such as Codex are not counted. Data: the JSON files in `.ai-usage/nf-exit-code-task-result/`.

| Phase | Agent · model · effort (calls) | Active time | Input + cache write | Cache read | Output | Est. cost |
| --- | --- | --- | --- | --- | --- | --- |
| other: research: Fovus status vs exit code | main · opus-5.5 · xhigh (19) | 5 min | 69,944 | 1,891,545 | 15,861 | $1.25 |
| implementation: exit-code-driven completion + handler test | main · opus-5.5 · xhigh (19) | 4 min | 124,128 | 2,761,436 | 26,969 | $2.08 |
| other: open PR + AI usage record | main · opus-5.5 · xhigh (8) | 1 min | 12,249 | 1,407,680 | 6,219 | $0.50 |
| **Total** | 46 calls | 10 min | 206,321 | 6,060,661 | 49,049 | $3.84 |

Skills (a model call belongs to the skill launched last since the person's last message; `none` is work outside any skill):

| Skill | Launches | Model calls | Active time | Est. cost | Did it help? |
| --- | --- | --- | --- | --- | --- |
| none | 0 | 22 | 5 min | $2.06 | - |
| test-driven-development | 1 | 18 | 4 min | $1.40 | helped: Forced a first handler-level test for checkIfCompleted; it failed for the right reason (Job job-1 failed) before the fix |
| fovus-ai-usage | 1 | 6 | 1 min | $0.38 | neutral: Bookkeeping only; small cost |

Tools, by the tokens their results added to the context (an estimate, 4 characters a token):

| Tool | Calls | Failed | Time | Result tokens |
| --- | --- | --- | --- | --- |
| Bash: grep | 10 | 1 | 0.1 min | 7,212 |
| Bash: wc | 1 | 0 | 0.0 min | 4,510 |
| Read | 3 | 0 | 0.0 min | 3,569 |
| Bash: cat | 3 | 0 | 0.1 min | 2,899 |
| Bash: git ls-files | 2 | 1 | 0.0 min | 2,475 |
| Bash: sed | 2 | 0 | 0.0 min | 1,666 |
| Bash: ls | 1 | 1 | 0.0 min | 1,306 |
| Bash: python3 | 6 | 1 | 0.1 min | 846 |
| Bash: git status | 1 | 0 | 0.0 min | 774 |
| Bash: git diff | 1 | 0 | 0.0 min | 408 |
| Bash: mkdir | 1 | 0 | 0.0 min | 378 |
| Bash: git switch | 1 | 0 | 0.0 min | 295 |
| Bash: gradlew | 2 | 0 | 0.6 min | 235 |
| mcp__ccd_turn__wrap_up | 4 | 0 | 0.0 min | 185 |
| Bash: find | 1 | 0 | 0.0 min | 153 |
| 6 other tools | 15 | 0 | 0.2 min | 397 |

- Tool calls: 54, 4 failed (7%)

Why tool calls failed:

| Reason | Count | Tools | What happened |
| --- | --- | --- | --- |
| shell_glob | 2 | Bash: git ls-files (1), Bash: ls (1) | Unquoted globs (grep --include=*.groovy, ls .github/PULL_REQUEST_TEMPLATE*) fail in zsh when nothing matches; quote the pattern or set null_glob |
| other | 1 | Bash: grep (1) | sed -n with a line number from a grep that matched nothing became an invalid range; check the grep pattern against the real signature first, or read the file with an offset |
| shell_syntax | 1 | Bash: python3 (1) | echo ====== fails in zsh because a leading = expands to a command path; print a separator that does not start with = |
- Test runs: none
- In-session reviews: none

- Messages from the person: 5
- Loop and slash-command passes: 0
- Subagent calls: 0
- Waiting on the person: 3 min
- Times the person interrupted: 0

Notes: The first handler-test run failed in setup for all 12 cases: a bare TaskRun needs a TaskProcessor stub because FovusJobConfig reads task.outputFilesNames. One extra test run.

Lessons for next time:

- Window 2 (implementation, $2.08): there was no test that built a FovusTaskHandler, so the fixture had to be discovered by trial (job config JSON, processor stub). FovusTaskHandlerTest.runningHandler is now that fixture; reuse it for future handler changes.
