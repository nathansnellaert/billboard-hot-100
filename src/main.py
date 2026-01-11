from subsets_utils import DAG, validate_environment
from nodes import charts

workflow = DAG({
    charts.run: [],
})


def main():
    validate_environment()
    workflow.run()


if __name__ == "__main__":
    main()
