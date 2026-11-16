# 3rdparty/BUILD
python_requirements(
    name="reqs",
    source="requirements.txt",
    resolve="shared-lock",
)

python_sources(
    name="lock_tools",
    sources=["extract_lock_constraints.py"],
)

files(
    name="lock_files",
    sources=["requirements.txt", "constraints.txt", "user_reqs.lock", "extract_lock_constraints.py"],
)
