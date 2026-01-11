from subsets_utils import DAG, validate_environment
from nodes import ingest, charts

workflow = DAG({
    ingest.run: [],
    charts.run: [ingest.run],
})


def main():
    validate_environment()
    workflow.run()
    workflow.save_state()


if __name__ == "__main__":
    main()
