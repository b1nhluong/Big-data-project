#!/usr/bin/env python3
"""Đưa phần việc của một thành viên từ repo tham chiếu sang repo chính, commit bằng tài khoản của chính người đó.

Cách dùng (chạy trong thư mục repo chính, trên nhánh main):
    python3.11 scripts/port.py tv2 --dry-run   # xem trước sẽ lấy file nào, chia thành những commit nào
    python3.11 scripts/port.py tv2             # lấy file, hỏi trước từng commit, đẩy lên main
    python3.11 scripts/port.py --status        # tiến độ cả nhóm

Mặc định lấy toàn bộ phần việc tới checkpoint 26/10. Sau này lấy thêm: --upto 6 (tới tuần 6).
Windows: thay `python3.11` bằng `py -3.11`.

Script đọc danh mục file `docs/ke-hoach/file-index.md` trong repo tham chiếu, lấy đúng các file của
người đó, gom theo việc thành từng commit và hỏi trước mỗi commit. Không có gì được đẩy lên GitHub
cho tới khi bạn đồng ý ở bước cuối.
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
CHECKPOINT_UPTO = 4   # checkpoint 26/10: phần việc tới hết tuần 4

# File "(đi kèm)" không tự khớp được với commit nào hợp lý: gắn vào commit của file mốc.
ATTACH_TO = {
    "batch/eda.py": "docs/eda.md",
    "ingestion/chaos.py": "ingestion/replayer.py",
    "infra/docker/replayer/Dockerfile": "ingestion/replayer.py",
    "infra/k8s/jobs/replayer.yaml": "ingestion/replayer.py",
}
# Đổi tuần so với file-index: test exactly-once import streaming.kpi_minute (tuần 4).
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
# CI chỉ chạy pytest khi code của cả nhóm đã có trên main, nếu không CI sẽ đỏ.
PYTEST_CI_MSG = "ci: run unit and integration tests with pytest"
CODE_PREFIXES = ("shoppulse/", "ingestion/", "batch/", "streaming/", "ml/", "serving/", "tests/", "conf/",
                 "requirements", "pytest.ini")

# CI ban đầu (TV1): chưa có requirements và test, nên chỉ chạy ruff + kubeconform (khi đã có infra/k8s).
# TV5 bật pytest bằng ci.yml đầy đủ của repo tham chiếu khi code của cả nhóm đã có.
CI_EARLY = """\
# Chạy mỗi push/PR: lint + kiểm tra manifest K8s. TV5 bật pytest khi code của cả nhóm đã có.
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
        die("Bạn đã dừng. Những commit đã tạo vẫn nằm trên máy bạn, chưa đẩy lên GitHub. "
            "Chạy lại script để làm tiếp.")
    return ans or default


def head_files() -> set[str]:
    return set(git("ls-tree", "-r", "--name-only", "HEAD").splitlines())


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
    entries.sort(key=lambda e: e.week)   # ổn định: giữ thứ tự trong cùng tuần
    return entries


@dataclass
class Group:
    message: str
    files: list[str] = field(default_factory=list)
    rewrite: list[str] = field(default_factory=list)
    special: dict[str, str] = field(default_factory=dict)  # path -> nội dung thay cho bản tham chiếu
    gated: bool = False                                     # chỉ làm khi code cả nhóm đã có


def common_prefix_len(a: str, b: str) -> int:
    n = 0
    for x, y in zip(a.split("/")[:-1], b.split("/")[:-1]):
        if x != y:
            break
        n += 1
    return n


def build_groups(entries: list[Entry], member: str, upto: int) -> tuple[list[Group], list[str]]:
    mine = [e for e in entries if e.member == member and e.week <= upto]
    groups: "OrderedDict[str, Group]" = OrderedDict()
    owner_of: dict[str, str] = {}

    def add(msg: str, e: Entry) -> None:
        g = groups.setdefault(msg, Group(msg))
        if e.path in g.files:
            return
        g.files.append(e.path)
        owner_of[e.path] = msg
        if e.rewrite:
            g.rewrite.append(e.path)

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
        if ".github/workflows/ci.yml" in g.files:
            g.special[".github/workflows/ci.yml"] = CI_EARLY
    if member == "tv5" and upto >= 3:
        out.append(Group(PYTEST_CI_MSG, [".github/workflows/ci.yml"], gated=True))
    results = [e.path for e in mine if e.kind == "result"]
    return out, results


def missing_for_pytest(entries: list[Entry], upto: int) -> dict[str, list[str]]:
    """Code của các thành viên (tới tuần upto) chưa có trên HEAD: {thành viên: [file]}."""
    have, out = head_files(), {}
    for e in entries:
        if e.week <= upto and e.kind != "result" and not e.rewrite and e.path.startswith(CODE_PREFIXES):
            if e.path not in have:
                out.setdefault(e.member, []).append(e.path)
    return out


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
    # File phải viết lại mà người dùng đã có bản riêng trên máy (vd. docs/eda.md vừa sinh từ dữ liệu thật): giữ nguyên.
    keep = {f for f in group.rewrite if os.path.exists(f)}
    for f in sorted(keep):
        say(f"    giữ bản trên máy của bạn: {f}")
    plain = [f for f in group.files if f not in group.special and f != "README.md" and f not in keep]
    if plain:
        git("checkout", REF_BRANCH, "--", *plain)
    for path, content in group.special.items():
        write_file(path, content)
    if "README.md" in group.files and "README.md" not in group.special:
        write_file("README.md", adapt_readme(ref_content("README.md")))


def wanted(group: Group, f: str) -> str:
    if f in group.special:
        return group.special[f]
    return adapt_readme(ref_content(f)) if f == "README.md" else ref_content(f)


def same_as_head(group: Group) -> bool:
    """True nếu mọi file của nhóm đã có trong HEAD với đúng nội dung sẽ commit."""
    for f in group.files:
        if not git_ok("cat-file", "-e", f"HEAD:{f}"):
            return False
        if git("show", f"HEAD:{f}").strip() != wanted(group, f).strip():
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
        say("    (chưa cài ruff, bỏ qua kiểm tra. Cài bằng: pip install ruff)")
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
    say("Đang tải repo tham chiếu...")
    git("fetch", "-q", "ref")
    if "origin" in remotes:
        git("fetch", "-q", "origin", check=False)


def load_entries() -> list[Entry]:
    return parse_index(ref_content(INDEX_PATH))


def sync_main() -> None:
    """Đứng trên main và kéo bản mới nhất về."""
    if git("status", "--porcelain", "--untracked-files=no"):
        die("Có thay đổi chưa commit trong repo. Chạy `git stash` rồi chạy lại script (xong thì `git stash pop`).")
    if git("rev-parse", "--abbrev-ref", "HEAD") != "main":
        git("switch", "-q", "main")
    if git_ok("rev-parse", "--verify", "-q", "origin/main"):
        git("pull", "-q", "--rebase", "origin", "main")


def push_main() -> None:
    for _ in range(3):
        if not git_ok("pull", "-q", "--rebase", "origin", "main"):
            git("rebase", "--abort", check=False)
            die("Không gộp được với bản mới nhất trên GitHub (có người sửa cùng file). Báo TV1.")
        if git_ok("push", "-q", "origin", "main"):
            say("Đã đẩy lên GitHub (nhánh main).")
            return
    die("Đẩy lên không thành công sau 3 lần. Chạy lại: git pull --rebase origin main && git push origin main")


def cmd_status(upto: int) -> None:
    ensure_ref()
    entries = load_entries()
    base = os.environ.get("PORT_BASE") or ("origin/main" if git_ok("rev-parse", "--verify", "-q", "origin/main") else "HEAD")
    have = set(git("ls-tree", "-r", "--name-only", base).splitlines())
    say(f"\nTiến độ trên {base}, phần việc tới tuần {upto} (không tính file kết quả):\n")
    missing = []
    for m in MEMBERS:
        files = [e for e in entries if e.member == m and e.week <= upto and e.kind != "result"]
        done = sum(e.path in have for e in files)
        mark = "xong" if done == len(files) else f"{done}/{len(files)} file"
        authors = git("log", base, "--format=%an", "--", *[e.path for e in files], check=False) if files else ""
        names = ", ".join(sorted(set(authors.splitlines()))) or "chưa ai commit"
        say(f"  {m.upper()}: {mark:<12} người commit: {names}")
        missing += [e for e in files if e.path not in have]
    ci = git("show", f"{base}:.github/workflows/ci.yml", check=False)
    say("\nCI chạy pytest: " + ("đã bật" if "pytest" in ci else "chưa (TV5 bật khi code cả nhóm đã có)"))
    if missing:
        say("\nCòn thiếu:")
        for e in missing[:40]:
            say(f"  {e.member.upper()} · {e.path}")
        if len(missing) > 40:
            say(f"  ... và {len(missing) - 40} file nữa")


def cmd_port(member: str, upto: int, dry: bool, auto: bool, push: bool | None) -> None:
    ensure_ref()
    entries = load_entries()
    if not dry:
        sync_main()
    groups, results = build_groups(entries, member, upto)
    if not groups and not results:
        say(f"\n{member.upper()} không có file nào tới tuần {upto}.")
        return

    name, email = git("config", "user.name", check=False), git("config", "user.email", check=False)
    say(f"\n{member.upper()} · commit sẽ mang tên: {name or '(chưa đặt)'} <{email or '(chưa đặt)'}>")
    if not name or not email:
        die('Chưa đặt tên/email git. Chạy: git config --global user.name "Tên"  và  '
            'git config --global user.email "email-GitHub-của-bạn"')
    if member != "tv2" and "shoppulse/config.py" not in head_files():
        say("Lưu ý: TV2 chưa đẩy phần lõi shoppulse/. Vẫn commit được, nhưng chạy thử code sẽ lỗi tới khi TV2 đẩy.")

    say("\nSẽ tạo các commit sau:")
    for i, g in enumerate(groups, 1):
        done = same_as_head(g)
        note = "   (đã có, bỏ qua)" if done else ("   (chỉ làm khi code cả nhóm đã có trên main)" if g.gated else "")
        say(f"  {i}. {g.message}{note}")
        for f in g.files:
            tag = "  ← PHẢI sửa số liệu" if f in g.rewrite else ("  ← bản chưa chạy test" if f in g.special else "")
            say(f"       {f}{tag}")
    if results:
        say("  Không chép (tự chạy job rồi commit kết quả thật):")
        for f in results:
            say(f"       {f}")
    if dry:
        say("\n--dry-run: chưa thay đổi gì.")
        return

    made = 0
    skipped_rewrite: list[str] = []
    for i, g in enumerate(groups, 1):
        if same_as_head(g):
            continue
        say(f"\n[{i}/{len(groups)}] {g.message}")
        if g.gated:
            missing = missing_for_pytest(entries, upto)
            if missing:
                say("  Chưa bật được: còn thiếu code của " + ", ".join(m.upper() for m in sorted(missing)) +
                    ". Khi mọi người đã đẩy xong, chạy lại lệnh này để bật.")
                continue
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
            say("  Chưa commit. Sửa lỗi rồi chạy lại script; các file đã lấy vẫn nằm trong thư mục.")
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

    ahead = git("rev-list", "--count", "origin/main..HEAD", check=False) if git_ok(
        "rev-parse", "--verify", "-q", "origin/main") else str(made)
    say(f"\nĐã tạo {made} commit mới. Chưa đẩy lên GitHub: {ahead or 0} commit.")
    if results:
        say("\nViệc còn lại: chạy job trên dữ liệu thật để sinh file kết quả, rồi commit:")
        for f in results:
            stem = os.path.splitext(os.path.basename(f))[0]
            say(f"  {f}:  SHOPPULSE_PROFILE=local {RESULT_JOBS.get(stem, 'xem scripts/run_local_pipeline.sh')}")
        say('  sau đó: git add <file> && git commit -m "bench: <mô tả> on real data" && git push')
    if skipped_rewrite:
        say("\nFile phải sửa số liệu chưa commit: " + ", ".join(skipped_rewrite) +
            ". Có số liệu thì chạy lại đúng lệnh này.")
    if not ahead or ahead == "0":
        return
    do_push = push if push is not None else (ask("\nĐẩy lên GitHub?", "y", auto) == "y")
    if do_push:
        push_main()
    else:
        say("\nKhi sẵn sàng: git pull --rebase origin main && git push origin main")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="Đưa phần việc từ repo tham chiếu sang repo chính.")
    p.add_argument("member", nargs="?", help="tv1..tv5")
    p.add_argument("--status", action="store_true", help="xem tiến độ cả nhóm")
    p.add_argument("--upto", type=int, default=CHECKPOINT_UPTO, help="lấy phần việc tới tuần này (mặc định 4)")
    p.add_argument("--dry-run", action="store_true", help="chỉ xem trước, không thay đổi gì")
    p.add_argument("--yes", action="store_true", help="không hỏi (đồng ý tất cả)")
    p.add_argument("--no-push", action="store_true", help="không đẩy lên GitHub")
    a = p.parse_args()
    if a.status:
        cmd_status(a.upto)
        return
    if not a.member:
        p.print_help()
        sys.exit(2)
    member = a.member.lower()
    if member not in MEMBERS:
        die("Thành viên phải là tv1, tv2, tv3, tv4 hoặc tv5.")
    cmd_port(member, a.upto, a.dry_run, a.yes, False if a.no_push else None)


if __name__ == "__main__":
    main()
