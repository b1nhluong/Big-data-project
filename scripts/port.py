#!/usr/bin/env python3
"""Đưa phần việc của một thành viên từ repo tham chiếu sang repo chính, rồi commit bằng tài khoản của chính người đó.

Cách dùng (chạy trong thư mục repo chính):
    python3.11 scripts/port.py tv2 1            # TV2, tuần 1
    python3.11 scripts/port.py tv2 1 --dry-run  # chỉ xem sẽ lấy file nào, không đổi gì
    python3.11 scripts/port.py --status         # tiến độ cả nhóm, tuần 1-4

Windows: thay `python3.11` bằng `py -3.11`.

Script đọc danh mục file `docs/ke-hoach/file-index.md` trong repo tham chiếu, lấy đúng các file của
người đó trong tuần đó, gom theo commit gợi ý và hỏi trước từng commit. Không có gì được đẩy lên
GitHub cho tới khi bạn đồng ý ở bước cuối.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from collections import OrderedDict
from dataclasses import dataclass, field

REF_URL = "https://github.com/b1nhluong/big_data_claude.git"
REF_BRANCH = "ref/main"
INDEX_PATH = "docs/ke-hoach/file-index.md"
MEMBERS = ["tv1", "tv2", "tv3", "tv4", "tv5"]

# File "(đi kèm)" không tự khớp được với commit nào hợp lý: gắn vào commit của file mốc.
ATTACH_TO = {
    "batch/eda.py": "docs/eda.md",
    "ingestion/chaos.py": "ingestion/replayer.py",
    "infra/docker/replayer/Dockerfile": "ingestion/replayer.py",
    "infra/k8s/jobs/replayer.yaml": "ingestion/replayer.py",
}
# Đổi tuần so với file-index: test exactly-once import streaming.kpi_minute (tuần 4),
# để ở tuần 3 thì pytest trong CI đỏ. Cùng người (TV3), chỉ dời sang tuần 4.
WEEK_OVERRIDE = {
    "tests/integration/test_streaming_exactly_once.py": 4,
}
# File "(đi kèm)" có commit riêng.
OWN_COMMIT = [
    (("docs/plan.md", "docs/weekly/", "docs/ke-hoach/"), "docs(plan): 12-week plan, weekly tasks and porting guide"),
]
# Lệnh sinh ra file kết quả (không chép từ repo tham chiếu).
RESULT_JOBS = {
    "csv_to_parquet": "python -m ingestion.csv_to_parquet",
    "silver_dq": "python -m batch.silver",
    "ml_als": "python -m ml.als_recommender",
    "ml_propensity": "python -m ml.purchase_propensity",
    "reconciliation": "python -m batch.reconciliation",
    "optimization": "python -m batch.optimization --repeat 3",
    "mapreduce_vs_spark": "xem bước 7 trong scripts/run_local_pipeline.sh",
}

# Gợi ý cách lấy số liệu cho file phải viết lại.
REWRITE_HINTS = {
    "docs/eda.md": "khi đã có bronze thật, chạy SHOPPULSE_PROFILE=local python -m batch.eda --fraction 0.01 để sinh lại file",
    "docs/report/bao-cao.md": "mục 1 dùng số thật: 109.950.743 sự kiện tháng 10-11/2019, CSV 13,7 GB",
}

# CI tuần 1-2: chưa có requirements và test, nên chỉ chạy ruff + kubeconform (khi đã có infra/k8s).
# TV5 bật pytest ở tuần 3 bằng ci.yml đầy đủ của repo tham chiếu.
CI_EARLY = """\
# Chạy mỗi push/PR: lint + kiểm tra manifest K8s. TV5 thêm job pytest ở tuần 3.
name: ci
on:
  push: {branches: [main]}
  pull_request:
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.11"}
      - run: pip install "ruff>=0.6"
      - run: ruff check --select F,E9 --line-length 130 .
  manifests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: kubeconform
        run: |
          if [ ! -d infra/k8s ]; then echo "Chưa có infra/k8s, bỏ qua"; exit 0; fi
          curl -sSL https://github.com/yannh/kubeconform/releases/download/v0.6.7/kubeconform-linux-amd64.tar.gz | tar xz
          ./kubeconform -summary -ignore-missing-schemas -skip CustomResourceDefinition \\
            -ignore-filename-pattern 'values\\.yaml$' -ignore-filename-pattern 'strimzi-values' \\
            -ignore-filename-pattern 'spark-operator-values' -ignore-filename-pattern 'kube-prometheus-stack-values' \\
            infra/k8s
"""

README_NOTE = """\
> Repo chính của nhóm. Mỗi thành viên đưa phần việc của mình vào, chạy kiểm tra rồi commit bằng tài khoản của mình.
> Mã nguồn được phát triển từ một bản tham chiếu dựng với sự hỗ trợ của công cụ AI
> (cách làm: [docs/ke-hoach/porting-guide.md](docs/ke-hoach/porting-guide.md)). Số liệu trong repo này do nhóm tự đo.
"""


# ---------------------------------------------------------------- tiện ích

def say(msg: str = "") -> None:
    print(msg, flush=True)


def die(msg: str) -> None:
    say(f"\n[DỪNG] {msg}")
    sys.exit(1)


def git(*args: str, check: bool = True, capture: bool = True) -> str:
    res = subprocess.run(["git", *args], capture_output=capture, text=True, encoding="utf-8", errors="replace")
    if check and res.returncode != 0:
        die(f"git {' '.join(args)} lỗi:\n{(res.stderr or res.stdout or '').strip()}")
    return (res.stdout or "").strip() if capture else ""


def git_ok(*args: str) -> bool:
    return subprocess.run(["git", *args], capture_output=True).returncode == 0


def ask(question: str, default: str = "y", auto: bool = False) -> str:
    if auto:
        return default
    hint = "[Y/n/q]" if default == "y" else "[y/N/q]"
    try:
        ans = input(f"{question} {hint} ").strip().lower()
    except EOFError:
        ans = ""
    if ans == "q":
        die("Bạn đã dừng. Những commit đã tạo vẫn nằm trên nhánh hiện tại (chưa đẩy lên GitHub).")
    return ans or default


# ---------------------------------------------------------------- đọc danh mục file

@dataclass
class Entry:
    member: str
    week: int
    path: str
    kind: str          # commit | attach | result
    message: str = ""
    rewrite: bool = False


def parse_index(text: str) -> list[Entry]:
    entries, week = [], 0
    for line in text.splitlines():
        m = re.match(r"^## Tuần (\d+)", line)
        if m:
            week = int(m.group(1))
            continue
        m = re.match(r"^\| (TV\d) \| `([^`]+)` \| (.*) \|\s*$", line)
        if not (m and week):
            continue
        member, path, note = m.group(1).lower(), m.group(2), m.group(3)
        week_of = WEEK_OVERRIDE.get(path, week)
        rewrite = "viết lại" in note
        if "tự chạy job" in note:
            entries.append(Entry(member, week_of, path, "result"))
        elif note.startswith("(đi kèm)"):
            entries.append(Entry(member, week_of, path, "attach", rewrite=rewrite))
        else:
            msg = re.match(r"`([^`]+)`", note)
            entries.append(Entry(member, week_of, path, "commit", msg.group(1) if msg else "chore: add files", rewrite))
    return entries


@dataclass
class Group:
    message: str
    files: list[str] = field(default_factory=list)
    rewrite: list[str] = field(default_factory=list)
    special: dict[str, str] = field(default_factory=dict)  # path -> nội dung thay cho bản tham chiếu


def common_prefix_len(a: str, b: str) -> int:
    n = 0
    for x, y in zip(a.split("/")[:-1], b.split("/")[:-1]):
        if x != y:
            break
        n += 1
    return n


def build_groups(entries: list[Entry], member: str, week: int) -> tuple[list[Group], list[str]]:
    mine = [e for e in entries if e.member == member and e.week == week]
    groups: "OrderedDict[str, Group]" = OrderedDict()
    owner_of: dict[str, str] = {}

    def add(msg: str, e: Entry) -> None:
        g = groups.setdefault(msg, Group(msg))
        g.files.append(e.path)
        owner_of[e.path] = msg
        if e.rewrite:
            g.rewrite.append(e.path)

    # Tuần 1-2: ci.yml rút gọn, commit riêng như trong docs/weekly/week-01.md
    for e in mine:
        if e.kind == "commit":
            add("ci: add ruff and kubeconform checks" if e.path == ".github/workflows/ci.yml" else e.message, e)
    for e in mine:
        if e.kind != "attach":
            continue
        own = next((msg for prefixes, msg in OWN_COMMIT if e.path.startswith(prefixes)), None)
        if own:
            add(own, e)
            continue
        anchor = ATTACH_TO.get(e.path)
        if anchor and anchor in owner_of:
            add(owner_of[anchor], e)
            continue
        best, best_len = None, -1
        for msg, g in groups.items():
            score = max((common_prefix_len(e.path, f) for f in g.files), default=0)
            if score > best_len:
                best, best_len = msg, score
        add(best or "chore: add supporting files", e)

    out = list(groups.values())
    for g in out:
        if ".github/workflows/ci.yml" in g.files and week < 3:
            g.special[".github/workflows/ci.yml"] = CI_EARLY
    # TV5 tuần 3: bật pytest trong CI bằng ci.yml đầy đủ
    if member == "tv5" and week == 3:
        out.append(Group("ci: run unit and integration tests with pytest", [".github/workflows/ci.yml"]))
    results = [e.path for e in mine if e.kind == "result"]
    return out, results


# ---------------------------------------------------------------- biến đổi riêng

def adapt_readme(text: str) -> str:
    lines, out, i = text.splitlines(keepends=True), [], 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("> **Đây là repo tham chiếu.**"):
            while i < len(lines) and lines[i].startswith(">"):
                i += 1
            out.append(README_NOTE)
            continue
        if line.startswith("## Kết quả nổi bật"):
            i += 1
            while i < len(lines) and not lines[i].startswith("## "):
                i += 1
            continue
        out.append(line)
        i += 1
    return "".join(out)


def ref_content(path: str) -> str:
    return git("show", f"{REF_BRANCH}:{path}")


def write_file(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(content if content.endswith("\n") else content + "\n")


def checkout(group: Group) -> None:
    plain = [f for f in group.files if f not in group.special and f != "README.md"]
    if plain:
        git("checkout", REF_BRANCH, "--", *plain)
    for path, content in group.special.items():
        write_file(path, content)
    if "README.md" in group.files and "README.md" not in group.special:
        write_file("README.md", adapt_readme(ref_content("README.md")))


def same_as_head(group: Group) -> bool:
    """True nếu mọi file của nhóm đã có trong HEAD với đúng nội dung sẽ commit."""
    for f in group.files:
        if not git_ok("cat-file", "-e", f"HEAD:{f}"):
            return False
        head = git("show", f"HEAD:{f}")
        want = group.special.get(f)
        if want is None:
            want = adapt_readme(ref_content(f)) if f == "README.md" else ref_content(f)
        if head.strip() != want.strip():
            return False
    return True


def run_ruff(files: list[str]) -> bool:
    py = [f for f in files if f.endswith(".py")]
    if not py:
        return True
    cmd = None
    if shutil.which("ruff"):
        cmd = ["ruff"]
    elif subprocess.run([sys.executable, "-m", "ruff", "--version"], capture_output=True).returncode == 0:
        cmd = [sys.executable, "-m", "ruff"]
    if not cmd:
        say("    (chưa cài ruff, bỏ qua lint. Cài bằng: pip install ruff)")
        return True
    res = subprocess.run([*cmd, "check", "--select", "F,E9", "--line-length", "130", *py],
                         capture_output=True, text=True)
    if res.returncode == 0:
        say("    ruff: OK")
        return True
    say("    ruff báo lỗi:\n" + res.stdout)
    return False


# ---------------------------------------------------------------- các lệnh

def ensure_ref() -> None:
    if not git_ok("rev-parse", "--git-dir"):
        die("Hãy chạy script trong thư mục repo chính của nhóm (thư mục có .git).")
    remotes = git("remote").split()
    if "ref" not in remotes:
        say(f"Thêm remote 'ref' -> {REF_URL}")
        git("remote", "add", "ref", REF_URL)
    say("Đang tải repo tham chiếu (git fetch ref)...")
    git("fetch", "-q", "ref")
    if "origin" in remotes:
        git("fetch", "-q", "origin", check=False)


def load_entries() -> list[Entry]:
    return parse_index(ref_content(INDEX_PATH))


def origin_web_url() -> str:
    url = git("remote", "get-url", "origin", check=False)
    m = re.search(r"github\.com[:/](.+?)(\.git)?$", url)
    return f"https://github.com/{m.group(1)}" if m else ""


def cmd_status(weeks: list[int]) -> None:
    ensure_ref()
    entries = load_entries()
    base = os.environ.get("PORT_BASE") or ("origin/main" if git_ok("rev-parse", "--verify", "-q", "origin/main") else "HEAD")
    have = set(git("ls-tree", "-r", "--name-only", base).splitlines())
    branches = git("branch", "-r", "--format=%(refname:short)", check=False).splitlines()
    say(f"\nTiến độ trên {base} (đếm file đã có, không tính file kết quả):\n")
    say("Tuần  " + "  ".join(f"{m.upper():>9}" for m in MEMBERS))
    for w in weeks:
        cells = []
        for m in MEMBERS:
            files = [e.path for e in entries if e.member == m and e.week == w and e.kind != "result"]
            if not files:
                cells.append(f"{'—':>9}")
                continue
            done = sum(f in have for f in files)
            mark = "xong" if done == len(files) else f"{done}/{len(files)}"
            cells.append(f"{mark:>9}")
        say(f"{w:>4}  " + "  ".join(cells))
    pending = [b for b in branches if re.match(r"origin/tv\d/week-\d\d", b)]
    if pending:
        say("\nNhánh đã đẩy lên (kiểm tra PR đã merge chưa):")
        for b in pending:
            merged = git_ok("merge-base", "--is-ancestor", b, base)
            say(f"  {b:<28} {'đã merge' if merged else 'CHƯA merge'}")
    missing = []
    for w in weeks:
        for e in entries:
            if e.week == w and e.kind != "result" and e.path not in have:
                missing.append(e)
    if missing:
        say("\nCòn thiếu (người · tuần · file):")
        for e in missing[:60]:
            say(f"  {e.member.upper()} · tuần {e.week} · {e.path}")
        if len(missing) > 60:
            say(f"  ... và {len(missing) - 60} file nữa")


def cmd_port(member: str, week: int, dry: bool, auto: bool, push: bool | None) -> None:
    ensure_ref()
    entries = load_entries()
    groups, results = build_groups(entries, member, week)
    if not groups and not results:
        say(f"\n{member.upper()} không có file mới nào trong tuần {week}. "
            f"Xem việc của tuần này trong docs/weekly/week-{week:02d}.md.")
        return

    name, email = git("config", "user.name", check=False), git("config", "user.email", check=False)
    say(f"\n{member.upper()} · tuần {week} · commit sẽ mang tên: {name or '(chưa đặt)'} <{email or '(chưa đặt)'}>")
    if not name or not email:
        die('Chưa đặt tên/email git. Chạy: git config --global user.name "Tên"  và  '
            'git config --global user.email "email-GitHub-của-bạn"')

    # Cảnh báo phụ thuộc: file của các tuần trước chưa có trong repo
    have = set(git("ls-tree", "-r", "--name-only", "HEAD").splitlines())
    earlier = [e for e in entries if e.week < week and e.kind != "result" and e.path not in have]
    if earlier:
        who = sorted({f"{e.member.upper()} tuần {e.week}" for e in earlier})
        say("\nLưu ý: repo chưa có đủ file của các tuần trước: " + ", ".join(who) +
            ".\nVẫn commit được, nhưng test/chạy thử có thể lỗi cho tới khi các phần đó được merge.")

    say("\nKế hoạch:")
    for i, g in enumerate(groups, 1):
        done = same_as_head(g)
        say(f"  {i}. {g.message}{'   (đã có, bỏ qua)' if done else ''}")
        for f in g.files:
            tag = "  ← PHẢI sửa số liệu" if f in g.rewrite else ("  ← bản rút gọn cho tuần 1-2" if f in g.special else "")
            say(f"       {f}{tag}")
    if results:
        say("  Không chép (tự chạy job rồi commit kết quả thật):")
        for f in results:
            say(f"       {f}")
    if dry:
        say("\n--dry-run: không thay đổi gì.")
        return

    if git("status", "--porcelain", "--untracked-files=no"):
        die("Có thay đổi chưa commit trong repo. Commit hoặc `git stash` trước rồi chạy lại.")

    branch = f"{member}/week-{week:02d}"
    current = git("rev-parse", "--abbrev-ref", "HEAD")
    if current != branch:
        if current == "main" and git_ok("rev-parse", "--verify", "-q", "origin/main"):
            git("merge", "-q", "--ff-only", "origin/main", check=False)
        if git_ok("rev-parse", "--verify", "-q", branch):
            git("switch", "-q", branch)
        else:
            git("switch", "-q", "-c", branch)
    say(f"\nĐang ở nhánh {branch}")

    made = 0
    skipped_rewrite = []
    for i, g in enumerate(groups, 1):
        if same_as_head(g):
            continue
        say(f"\n[{i}/{len(groups)}] {g.message}")
        if ask("  Lấy các file này và commit?", "y", auto) != "y":
            say("  Bỏ qua.")
            continue
        if g.rewrite and auto:
            say("  (chế độ --yes: bỏ qua file phải sửa số liệu, làm tay sau)")
            skipped_rewrite += g.rewrite
            g = Group(g.message, [f for f in g.files if f not in g.rewrite], [], g.special)
        if not g.files:
            continue
        checkout(g)
        if g.rewrite:
            say("  Các file sau đang chứa số liệu của repo tham chiếu. Mở ra, thay bằng số nhóm đo, lưu lại:")
            for f in g.rewrite:
                say(f"     {f}" + (f"   (gợi ý: {REWRITE_HINTS[f]})" if f in REWRITE_HINTS else ""))
            if ask("  Sửa xong thì nhấn Enter. Chưa có số liệu thì gõ s để để lại, commit sau", "y") == "s":
                for f in g.rewrite:
                    git("reset", "-q", "HEAD", "--", f, check=False)  # git checkout ref đã đưa file vào index
                    if git_ok("cat-file", "-e", f"HEAD:{f}"):
                        git("checkout", "HEAD", "--", f)
                    elif os.path.exists(f):
                        os.remove(f)
                skipped_rewrite += g.rewrite
        for f in g.files:
            if f.endswith(".sh") and os.path.exists(f):
                git("update-index", "--add", "--chmod=+x", f, check=False)
        if not run_ruff(g.files) and ask("  Vẫn commit?", "n", auto) != "y":
            say("  Chưa commit. Sửa lỗi rồi chạy lại script, các file đã lấy vẫn nằm trong thư mục.")
            return
        files = [f for f in g.files if os.path.exists(f) and f not in skipped_rewrite]
        if not files:
            continue
        git("add", "--", *files)
        if not git("diff", "--cached", "--name-only"):
            say("  Không có gì thay đổi.")
            continue
        git("commit", "-q", "-m", g.message)
        made += 1
        say(f"  Đã commit: {git('log', '-1', '--format=%h %s')}")

    say(f"\nĐã tạo {made} commit trên nhánh {branch}.")
    if results:
        say("\nViệc còn lại của bạn: chạy job để sinh file kết quả thật, rồi commit:")
        for f in results:
            stem = os.path.splitext(os.path.basename(f))[0]
            say(f"  {f}:  SHOPPULSE_PROFILE=local {RESULT_JOBS.get(stem, 'xem scripts/run_local_pipeline.sh')}")
        say("  sau đó: git add benchmarks/results/<file>.json && git commit -m \"bench: <mô tả> on real data\"")
    if skipped_rewrite:
        say("\nFile phải sửa số liệu chưa commit: " + ", ".join(skipped_rewrite))
    if made == 0 and not git_ok("rev-parse", "--verify", "-q", f"origin/{branch}"):
        return

    do_push = push if push is not None else (ask("\nĐẩy nhánh lên GitHub?", "y", auto) == "y")
    if do_push:
        git("push", "-u", "origin", branch, capture=False)
        web = origin_web_url()
        if web:
            say(f"\nMở Pull Request tại: {web}/compare/main...{branch}?expand=1")
    else:
        say(f"\nKhi sẵn sàng: git push -u origin {branch}")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="Đưa phần việc từ repo tham chiếu sang repo chính.")
    p.add_argument("member", nargs="?", help="tv1..tv5")
    p.add_argument("week", nargs="?", type=int, help="số tuần, ví dụ 1")
    p.add_argument("--status", action="store_true", help="xem tiến độ cả nhóm")
    p.add_argument("--weeks", default="1-4", help="tuần cho --status, ví dụ 1-4")
    p.add_argument("--dry-run", action="store_true", help="chỉ xem kế hoạch")
    p.add_argument("--yes", action="store_true", help="không hỏi (đồng ý tất cả)")
    p.add_argument("--no-push", action="store_true", help="không đẩy lên GitHub")
    a = p.parse_args()
    if a.status:
        lo, _, hi = a.weeks.partition("-")
        cmd_status(list(range(int(lo), int(hi or lo) + 1)))
        return
    if not a.member or a.week is None:
        p.print_help()
        sys.exit(2)
    member = a.member.lower()
    if member not in MEMBERS:
        die("Thành viên phải là tv1, tv2, tv3, tv4 hoặc tv5.")
    cmd_port(member, a.week, a.dry_run, a.yes, False if a.no_push else None)


if __name__ == "__main__":
    main()
