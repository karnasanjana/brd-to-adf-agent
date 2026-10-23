"""Hand-built sample COVID-19 medallion spec (the shipped working example).

Used by the smoke test and as an offline fixture. Mirrors the COVID-19 JHU BRD:
Johns Hopkins CSSE daily CSVs over anonymous HTTP -> ADLS Gen2 bronze/silver/gold
(Managed Identity), with Bronze = Copy Activity (no transform), and
df_silver_transform / df_gold_aggregate Mapping Data Flows.

The agent is NOT limited to this dataset — this is just the reference BRD.
"""

from __future__ import annotations

from models.spec import (
    Activity,
    AuthKind,
    Column,
    DataFlow,
    DataFlowEndpoint,
    Dataset,
    DatasetKind,
    DatasetLocation,
    DerivedColumn,
    HttpSource,
    LinkedService,
    MedallionLayer,
    PipelineDef,
    PipelineSpec,
    SortKey,
    StorageAccount,
    Transform,
    TransformKind,
    Trigger,
    TriggerKind,
)

COVID_BRD = """\
Build a COVID-19 medallion pipeline in Azure Data Factory ONLY (no Databricks,
Synapse, or Fabric).

SOURCE: Johns Hopkins CSSE daily reports, anonymous HTTPS files at
https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/
with file pattern MM-DD-YYYY.csv, parameterised by report date.

STORAGE: ADLS Gen2 account 'covidlake' with bronze, silver, gold containers,
accessed with Managed Identity (no keys).

BRONZE: copy the raw daily CSV byte-for-byte into the bronze container. No
transformation.

SILVER: cast Confirmed/Deaths/Recovered/Active to integer (null -> 0), cast
Last_Update to timestamp, add an ingestion_date audit column, drop rows with a
null/empty Country_Region, and dedup on (Country_Region, Province_State,
Last_Update). Write Parquet to silver.

GOLD: sum Confirmed/Deaths/Recovered by Country_Region, compute
case_fatality_rate = total_deaths / total_confirmed * 100 (0.0 when total_confirmed
is 0), sort by total_confirmed descending. Write CSV to gold.

ORCHESTRATION: chain Bronze -> Silver -> Gold so a failed Bronze halts downstream
work. Provide a manual trigger and a daily schedule trigger.
"""

_JHU_BASE = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master"
_JHU_DAILY = "csse_covid_19_data/csse_covid_19_daily_reports"

_SOURCE_COLUMNS = [
    Column(name="Province_State", type="string"),
    Column(name="Country_Region", type="string"),
    Column(name="Last_Update", type="string"),
    Column(name="Confirmed", type="string"),
    Column(name="Deaths", type="string"),
    Column(name="Recovered", type="string"),
    Column(name="Active", type="string"),
]


def _silver_dataflow() -> DataFlow:
    return DataFlow(
        name="df_silver_transform",
        layer=MedallionLayer.silver,
        source=DataFlowEndpoint(stream_name="BronzeSource", dataset="ds_adls_bronze"),
        sink=DataFlowEndpoint(stream_name="SilverSink", dataset="ds_adls_silver"),
        source_columns=_SOURCE_COLUMNS,
        transforms=[
            Transform(
                kind=TransformKind.cast,
                name="CastAndClean",
                columns=[
                    DerivedColumn(name="Confirmed", expression="iifNull(toInteger(Confirmed), 0)"),
                    DerivedColumn(name="Deaths", expression="iifNull(toInteger(Deaths), 0)"),
                    DerivedColumn(name="Recovered", expression="iifNull(toInteger(Recovered), 0)"),
                    DerivedColumn(name="Active", expression="iifNull(toInteger(Active), 0)"),
                    DerivedColumn(name="Last_Update", expression="toTimestamp(Last_Update, 'yyyy-MM-dd HH:mm:ss')"),
                    DerivedColumn(name="ingestion_date", expression="currentTimestamp()"),
                ],
            ),
            Transform(
                kind=TransformKind.filter,
                name="FilterCountry",
                condition="!isNull(Country_Region) && length(trim(Country_Region)) > 0",
            ),
            Transform(
                kind=TransformKind.aggregate,
                name="Dedup",
                group_by=["Country_Region", "Province_State", "Last_Update"],
                columns=[
                    DerivedColumn(name="Confirmed", expression="first(Confirmed)"),
                    DerivedColumn(name="Deaths", expression="first(Deaths)"),
                    DerivedColumn(name="Recovered", expression="first(Recovered)"),
                    DerivedColumn(name="Active", expression="first(Active)"),
                    DerivedColumn(name="ingestion_date", expression="first(ingestion_date)"),
                ],
            ),
        ],
    )


def _gold_dataflow() -> DataFlow:
    return DataFlow(
        name="df_gold_aggregate",
        layer=MedallionLayer.gold,
        source=DataFlowEndpoint(stream_name="SilverSource", dataset="ds_adls_silver"),
        sink=DataFlowEndpoint(stream_name="GoldSink", dataset="ds_adls_gold"),
        source_columns=[
            Column(name="Country_Region", type="string"),
            Column(name="Confirmed", type="integer"),
            Column(name="Deaths", type="integer"),
            Column(name="Recovered", type="integer"),
        ],
        transforms=[
            Transform(
                kind=TransformKind.aggregate,
                name="AggByCountry",
                group_by=["Country_Region"],
                columns=[
                    DerivedColumn(name="total_confirmed", expression="sum(Confirmed)"),
                    DerivedColumn(name="total_deaths", expression="sum(Deaths)"),
                    DerivedColumn(name="total_recovered", expression="sum(Recovered)"),
                ],
            ),
            Transform(
                kind=TransformKind.derived,
                name="CaseFatality",
                columns=[
                    DerivedColumn(
                        name="case_fatality_rate",
                        expression="iif(total_confirmed == 0, 0.0, "
                        "toDouble(total_deaths) / total_confirmed * 100)",
                    )
                ],
            ),
            Transform(
                kind=TransformKind.sort,
                name="SortByConfirmed",
                order_by=[SortKey(column="total_confirmed", order="desc")],
            ),
        ],
    )


def sample_covid_spec() -> PipelineSpec:
    return PipelineSpec(
        name="covid_medallion",
        source=HttpSource(
            base_url=_JHU_BASE,
            relative_url_template=f"{_JHU_DAILY}/@{{dataset().date}}.csv",
            file_pattern="MM-DD-YYYY.csv",
            auth=AuthKind.anonymous,
        ),
        storage=StorageAccount(
            account_name="covidlake",
            account_url="https://covidlake.dfs.core.windows.net",
            containers=["bronze", "silver", "gold"],
            auth=AuthKind.managed_identity,
        ),
        linked_services=[
            LinkedService(
                name="ls_http_github",
                type="HttpServer",
                auth=AuthKind.anonymous,
                url=_JHU_BASE + "/",
            ),
            LinkedService(
                name="ls_adls_gen2",
                type="AzureBlobFS",
                auth=AuthKind.managed_identity,
                account_url="https://covidlake.dfs.core.windows.net",
            ),
        ],
        datasets=[
            Dataset(
                name="ds_http_covid_csv",
                kind=DatasetKind.delimited_text,
                linked_service="ls_http_github",
                relative_url=f"{_JHU_DAILY}/@{{dataset().date}}.csv",
                parameters=["date"],
                columns=_SOURCE_COLUMNS,
            ),
            Dataset(
                name="ds_adls_bronze",
                kind=DatasetKind.delimited_text,
                linked_service="ls_adls_gen2",
                location=DatasetLocation(container="bronze", folder="covid", file_name="daily.csv"),
                columns=_SOURCE_COLUMNS,
            ),
            Dataset(
                name="ds_adls_silver",
                kind=DatasetKind.parquet,
                linked_service="ls_adls_gen2",
                location=DatasetLocation(container="silver", folder="covid"),
            ),
            Dataset(
                name="ds_adls_gold",
                kind=DatasetKind.delimited_text,
                linked_service="ls_adls_gen2",
                location=DatasetLocation(container="gold", folder="covid", file_name="by_country.csv"),
            ),
        ],
        dataflows=[_silver_dataflow(), _gold_dataflow()],
        pipelines=[
            PipelineDef(
                name="pl_ingest_bronze",
                parameters=["report_date"],
                activities=[
                    Activity(
                        name="CopyHttpToBronze",
                        type="Copy",
                        layer=MedallionLayer.bronze,
                        source_dataset="ds_http_covid_csv",
                        sink_dataset="ds_adls_bronze",
                    )
                ],
            ),
            PipelineDef(
                name="pl_transform_silver",
                activities=[
                    Activity(name="RunSilverFlow", type="ExecuteDataFlow", data_flow="df_silver_transform")
                ],
            ),
            PipelineDef(
                name="pl_aggregate_gold",
                activities=[
                    Activity(name="RunGoldFlow", type="ExecuteDataFlow", data_flow="df_gold_aggregate")
                ],
            ),
            PipelineDef(
                name="pl_master_pipeline",
                parameters=["report_date"],
                activities=[
                    Activity(name="Bronze", type="ExecutePipeline", invoked_pipeline="pl_ingest_bronze"),
                    Activity(
                        name="Silver",
                        type="ExecutePipeline",
                        invoked_pipeline="pl_transform_silver",
                        depends_on=["Bronze"],
                    ),
                    Activity(
                        name="Gold",
                        type="ExecutePipeline",
                        invoked_pipeline="pl_aggregate_gold",
                        depends_on=["Silver"],
                    ),
                ],
            ),
        ],
        triggers=[
            Trigger(name="tr_manual", kind=TriggerKind.manual, pipeline="pl_master_pipeline"),
            Trigger(
                name="tr_daily_cron",
                kind=TriggerKind.schedule,
                pipeline="pl_master_pipeline",
                frequency="Day",
                interval=1,
                start_time="2024-01-01T02:00:00Z",
            ),
        ],
    )
