### RELEASE PROCESS

1. Update the version number in `setup.py` (example: `1.0.16`).

2. Commit and push to GitHub.

3. Go to https://github.com/qubole/qds-sdk-py/releases and draft a new release: add a tag (example: `v1.0.16`) and what changed since the last release.

4. Make sure you have Qubole's PyPI credentials stored in `~/.pypirc`

    ```
    [server-login]
    username:qubole
    password:
    ```

5. Upload to PyPI: `python setup.py sdist upload`
