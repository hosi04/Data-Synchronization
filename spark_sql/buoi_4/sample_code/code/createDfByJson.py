from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HomeWork").master("local[*]").config("spark.executor.memory", "8g").getOrCreate()

label_schema = StructType([
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("color", StringType(), True)
])

jsonSchema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", FloatType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]), True),
    StructField("repo", StructType([
        StructField("id", FloatType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]), True),

    StructField("payload", StructType([                                     # Payload
        StructField("action", StringType(), True),
        StructField("issue", StructType([
            StructField("url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", FloatType(), True),
            StructField("number", FloatType(), True),
            StructField("title", StringType(), True),
            StructField("user", StructType([                                # user
                StructField("login", StringType(), True),
                StructField("id", FloatType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),                                                  # End user
            StructField("labels", ArrayType(label_schema), True),
            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("assignee", StringType(), True),
            StructField("milestone", StringType(), True),
            StructField("comments", IntegerType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("body", StringType(), True),
        ]), True),
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
])


jsonFile = spark.read.schema(jsonSchema).json("../data/2015-03-01-17.json")

jsonFile.select("id",
                "type",
                "actor.id",
                "actor.login",
                "actor.gravatar_id",
                "actor.url",
                "actor.avatar_url",

                "repo.id",
                "repo.name",
                "repo.url",

                "payload.action",
                "payload.issue",

                "issue.url",
                "issue.labels_url",
                "issue.comments_url",
                "issue.events_url",
                "issue.html_url",
                "issue.id",
                "issue.number",
                "issue.title",

                "issue.user.login",
                "issue.user.id",
                "issue.user.avatar_url",
                "issue.user.gravatar_id",
                "issue.user.url",
                "issue.user.html_url",
                "issue.user.followers_url",
                "issue.user.following_url",
                "issue.user.gists_url",
                "issue.user.starred_url",
                "issue.user.subscriptions_url",
                "issue.user.organizations_url",
                "issue.user.repos_url",
                "issue.user.events_url",
                "issue.user.received_events_url",
                "issue.user.type",
                "issue.user.site_admin",

                "issue.labels.url",
                "issue.labels.name",
                "issue.labels.color",

                "issue.state",
                "issue.locked",
                "issue.assignee",
                "issue.milestone",
                "issue.comments",
                "issue.created_at",
                "issue.updated_at",
                "issue.closed_at",
                "issue.body",

                "public",
                "created_at"
                ).show(truncate=False)

