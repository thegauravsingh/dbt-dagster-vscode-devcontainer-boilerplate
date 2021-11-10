## An Intro to scheduling dbt with Dagster using vscode devcontainer.

### Inspired from folloiwng Slides and recording for the talk
- [Slides](./Sydney%20dbt%20meetup%20-%20Benoit%20Perigaud%20-%20dbt%20and%20Dagster.pdf)
- [Youtube recording](https://www.youtube.com/watch?v=FkCEBFY_6ow)

### Setup a docker environment for devcontainer
- all packages are mentioned in requirements.txt, just open in vscode, do shift + P and then rebuild and reopen in container

### Setup dbt
- Execute dbt_init.sql to setup db for dbt.
- Optional Local Setup your `profiles.yml` file with your database credentials. The dbt project name is `dbt_demo`.
- Try your dbt connection by opening a shell in the `dbt_demo` folder and running `dbt debug`
- Do a `dbt seed` in the same folder to make sure that the correct rights are setup to write to the DB

### Setup dagster
- Execute dagster_init.sql to setup db for dagster.
- Optional Local setup
    - Update the file `dbt_pipeline.py` with the correct paths for your dbt executable (you can do a `where dbt` while in the venv) and your dbt project
    - If you want to run dagster in a "persistent mode", set up the env variable DAGSTER_HOME (e.g. `export DAGSTER_HOME=~/.dagster` in bash/zsh)
    - Update the `workspace.yaml` file with the correct python path for your venv (running `where python` from your venv)
    - Run `dagit` and open the web interface
    - For storing results in a Postgres DB instead of SQLite, create/modify your file `DAGSTER_HOME/dagster.yaml` following all the "xxx_storage" entries in the example `dagster.yaml`
    - For setting up concurrency limits, modify `DAGSTER_HOME/dagster.yaml` to add a configuration for the run_coordinator