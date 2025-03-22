from pyspark.sql import functions as F
import os
import uuid
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable


# Data Cleaning

credentials_path = "/usr/local/airflow/include/service_account.json"
credentials_abs_path = os.path.abspath(credentials_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_abs_path

def correct_data_types(df):
    df = df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Quantity", F.col("Quantity").cast("int"))
    df = df.withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    df = df.withColumn("CustomerID", F.col("CustomerID").cast("int"))
    return df


def fill_customer_id(df):
    invoice_to_customer_mapping = df.filter(F.col("CustomerID").isNotNull()) \
        .select("InvoiceNo", "CustomerID") \
        .dropDuplicates(["InvoiceNo"]) \
        .withColumnRenamed("CustomerID", "MappedCustomerID")

    df_with_mapping = df.join(invoice_to_customer_mapping, on="InvoiceNo", how="left")

    filled_df = df_with_mapping.withColumn(
        "CustomerID",
        F.when(F.col("CustomerId").isNotNull(), F.col("CustomerID")).when(F.col("MappedCustomerID").isNotNull(), F.col("MappedCustomerID")).otherwise(-1)
    )

    return filled_df.drop("MappedCustomerID")

def fill_country(df):
    customer_to_country_mapping = df.filter(F.col("Country").isNotNull()) \
        .select("CustomerID", "Country") \
        .dropDuplicates(["CustomerID"]) \
        .withColumnRenamed("Country", "MappedCountry")

    df_with_mapping = df.join(customer_to_country_mapping, on="CustomerID", how="left")

    filled_df = df_with_mapping.withColumn(
        "Country",
        F.when(F.col("Country").isNotNull(), F.col("Country")).when(F.col("MappedCountry").isNotNull(), F.col("MappedCountry")).otherwise("Unknown")
    )

    return filled_df.drop("MappedCountry")

def fill_invoice_date(df):
    invoice_to_date_mapping = df.filter(F.col("InvoiceDate").isNotNull()) \
        .select("InvoiceNo", "InvoiceDate") \
        .dropDuplicates(["InvoiceNo"]) \
        .withColumnRenamed("InvoiceDate", "MappedInvoiceDate")

    df_with_mapping = df.join(invoice_to_date_mapping, on="InvoiceNo", how="left")

    filled_df = df_with_mapping.withColumn(
        "InvoiceDate",
        F.when(F.col("InvoiceDate").isNotNull(), F.col("InvoiceDate")).when(F.col("MappedInvoiceDate").isNotNull(), F.col("MappedInvoiceDate")).otherwise(F.lit("2227-01-01 00:00:00"))
    )

    return filled_df.drop("MappedInvoiceDate")

def fill_unit_price(df):
    description_to_unit_price_mapping = df.filter(F.col("UnitPrice").isNotNull()) \
        .select("Description", "UnitPrice") \
        .dropDuplicates(["Description"]) \
        .withColumnRenamed("UnitPrice", "MappedUnitPrice")

    df_with_mapping = df.join(description_to_unit_price_mapping, on="Description", how="left")

    filled_df = df_with_mapping.withColumn(
        "UnitPrice",
        F.when(F.col("UnitPrice").isNotNull(), F.col("UnitPrice")).when(F.col("MappedUnitPrice").isNotNull(), F.col("MappedUnitPrice")).otherwise(F.lit(0.0))
    )

    return filled_df.drop("MappedUnitPrice")

def fill_description(df):
    stock_code_to_description_mapping = df.filter(F.col("Description").isNotNull()) \
        .select("StockCode", "Description") \
        .dropDuplicates(["StockCode"]) \
        .withColumnRenamed("Description", "MappedDescription")

    df_with_mapping = df.join(stock_code_to_description_mapping, on="StockCode", how="left")

    filled_df = df_with_mapping.withColumn(
        "Description",
        F.when(F.col("Description").isNotNull(), F.col("Description")).when(F.col("MappedDescription").isNotNull(), F.col("MappedDescription")).otherwise(F.lit("Unknown"))
    )

    return filled_df.drop("MappedDescription")

def fill_invoice_no(df):
    invoice_mapping = df.filter(F.col("InvoiceNo").isNotNull()) \
        .select("InvoiceNo", "InvoiceDate", "CustomerID") \
        .dropDuplicates(["InvoiceNo"]) \
        .withColumnRenamed("InvoiceNo", "MappedInvoiceNo") \

    df_with_mapping = df.join(
        invoice_mapping,
        on = (["InvoiceDate", "CustomerID"]),
        how="left"
    )

    filled_df = df_with_mapping.withColumn(
        "InvoiceNo",
        F.when(F.col("InvoiceNo").isNotNull(), F.col("InvoiceNo"))
         .when(F.col("MappedInvoiceNo").isNotNull(), F.col("MappedInvoiceNo"))
         .otherwise(2727272727)
    )

    return filled_df.drop("MappedInvoiceNo", "MappedInvoiceDate", "MappedCustomerID")


def clean_descriptions(df):
    """
    Identify and fix problematic descriptions:
    1. Extract numbers from all descriptions
    2. Check if these numbers match valid StockCodes
    3. Fix descriptions where possible
    4. Set descriptions with "came coded as" patterns to null
    5. Remove remaining problematic rows
    """
    key_words = [
        "not", "did", "error", "add", "came coded", "coded as",
        "test", "fix", "fixed", "mix up", "came coded as",
        "throw", "dotcom", "set", "wrong",
        "?", "lost", "sold in set", "damage", "rusty", "thrown",
        "amazon", "ebay", "breakage", "wet", "historic", "put aside",
        "crushed", "crush", "smashed", "mouldy", "unsaleable", "showroom",
        "found", "adjustment", "faulty", "missing", "damages", "mailout",
        "sample", "samples", "check", "incorrect", "fault", "stock check",
        "michel oops", "can't", "cant", "adjust", "away", "display", "smell",
        "incorr", "problem", "broken", "manual", "crack", "cracked", "wrongly",
        "damaged", "sold as", "imported", "for online", "cargo order", "counted",
        "mixed", "to push", "dagamed"
    ]

    wrong_codes = [
        "coded", "marked"
    ]

    valid_df = df.filter(~F.col("Description").rlike("^\d+$"))
    valid_mapping = valid_df.select("StockCode", "Description").dropDuplicates(["StockCode"])

    extract_numbers_udf = F.udf(
        lambda s: ''.join(c for c in s if c.isdigit()) if s else "",
        returnType="string"
    )

    key_words_condition = None
    for term in key_words:
        if key_words_condition is None:
            key_words_condition = F.lower(F.col("Description")).contains(term.lower())
        else:
            key_words_condition = key_words_condition | F.lower(F.col("Description")).contains(term.lower())

    wrong_codes_condition = None
    for term in wrong_codes:
        if wrong_codes_condition is None:
            wrong_codes_condition = F.lower(F.col("Description")).contains(term.lower())
        else:
            wrong_codes_condition = wrong_codes_condition | F.lower(F.col("Description")).contains(term.lower())


    problematic_df = df.filter(key_words_condition | F.col("Description").rlike("^\d+$") | wrong_codes_condition)
    good_df = df.filter(~key_words_condition & ~F.col("Description").rlike("^\d+$") & ~wrong_codes_condition)

    problematic_df = problematic_df.withColumn("extracted_code", extract_numbers_udf(F.col("Description")))

    corrected_df = problematic_df.join(
        valid_mapping.withColumnRenamed("StockCode", "valid_stock_code")
                    .withColumnRenamed("Description", "valid_description"),
        problematic_df["extracted_code"] == F.col("valid_stock_code"),
        "left"
    )


    corrected_df = corrected_df.withColumn(
        "is_coded_as_pattern",
        F.lower(F.col("Description")).like("%came coded as%")
    )

    corrected_df = corrected_df.withColumn(
        "StockCode",
        F.when(F.col("valid_stock_code").isNotNull(),
              F.col("valid_stock_code")).otherwise(F.col("StockCode"))
    ).withColumn(
        "Description",
        F.when(F.col("is_coded_as_pattern") & F.col("valid_stock_code").isNotNull(),
              F.lit(None))
        .when(F.col("valid_description").isNotNull(),
              F.col("valid_description"))
        .otherwise(F.col("Description"))
    )

    fixed_rows = corrected_df.filter(F.col("valid_stock_code").isNotNull())
    fixed_rows = fixed_rows.select(*df.columns)

    result_df = good_df.union(fixed_rows)

    return result_df

def special_stock_codes(df):
    special_codes = ["DOT", "POST", "D"]

    df = df.withColumn(
        "isSpecialCode",
        F.when(F.col("StockCode").isin(special_codes), True).otherwise(False)
    )

    return df

def handle_correction_invoices(df):
    df = df.withColumn(
        "IsCorrection",
        F.when(F.col("InvoiceNo").startswith("C"), True).otherwise(False)
    )

    return df

def product_sales(df):
    product_sales_df = df.groupBy("StockCode").agg(
        F.sum("SalesAmount").alias("TotalProductSales")
    )
    return product_sales_df

def rfm(df):

    filtered_df = df.filter(F.col("CustomerID") != -1)
    reference_date = filtered_df.select(F.max("InvoiceDate").alias("MaxDate")).collect()[0]["MaxDate"]

    customer_purchases = filtered_df.groupBy("CustomerID", "InvoiceNo").agg(
        F.sum("SalesAmount").alias("NetInvoiceAmount"),
        F.min("InvoiceDate").alias("InvoiceDate"),
        F.max("IsCorrection").alias("IsCorrection")
    )

    rfm_df = customer_purchases.groupBy("CustomerID").agg(
        F.min("InvoiceDate").alias("first_purchase"),
        F.sum(F.when((~F.col("IsCorrection")) & (F.col("NetInvoiceAmount") > 0), 1).otherwise(0)).alias("frequency"),
        F.max("InvoiceDate").alias("last_purchase"),
        F.sum("NetInvoiceAmount").alias("monetary")
    ).withColumn("recency", F.datediff(F.lit(reference_date), F.col("last_purchase")))

    return rfm_df

def rfm_segments(rfm_df):
    r_quantiles = rfm_df.approxQuantile("recency", [0.25, 0.5, 0.75], 0.01)
    f_quantiles = rfm_df.approxQuantile("frequency", [0.25, 0.5, 0.75], 0.01)
    m_quantiles = rfm_df.approxQuantile("monetary", [0.25, 0.5, 0.75], 0.01)

    rfm_score = rfm_df.withColumn(
        "R_tier",
        F.when(F.col("recency") <= r_quantiles[0], 4)
        .when(F.col("recency") <= r_quantiles[1], 3)
        .when(F.col("recency") <= r_quantiles[2], 2)
        .otherwise(1)
    ).withColumn(
        "F_tier",
        F.when(F.col("frequency") <= f_quantiles[0], 1)
        .when(F.col("frequency") <= f_quantiles[1], 2)
        .when(F.col("frequency") <= f_quantiles[2], 3)
        .otherwise(4)
    ).withColumn(
        "M_tier",
        F.when(F.col("monetary") <= m_quantiles[0], 1)
        .when(F.col("monetary") <= m_quantiles[1], 2)
        .when(F.col("monetary") <= m_quantiles[2], 3)
        .otherwise(4)
    )

    rfm_segments = rfm_score.withColumn(
        "RFM_Score",
        F.concat(F.col("R_tier"), F.col("F_tier"), F.col("M_tier"))
    ).withColumn(
        "RFM_Segment",
        F.when(F.col("RFM_Score").isin("444"), "Best Customers")
        .when(F.col("RFM_Score").isin("441", "442"), "High Spending New Customers")
        .when(F.col("RFM_Score").isin("443", "434", "433", "424"), "Low-Spending Active Loyal Customer")
        .when(F.col("RFM_Score").isin("144", "134", "124", "123"), "Churned Best Customers")
        .otherwise("Others")
    )

    return rfm_segments

class FeatureEngineeringTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, uid=None):
        super(FeatureEngineeringTransformer, self).__init__()
        self.uid = uid or "FeatureEngTransformer_" + str(uuid.uuid4())

    def _transform(self, df):
        df = df.withColumn("SalesDate", F.to_date("InvoiceDate")) \
            .withColumn("SalesTime", F.date_format("InvoiceDate", "HH:mm:ss"))

        df = df.withColumn("SalesAmount", F.col("Quantity") * F.col("UnitPrice"))

        df = df.withColumn("SalesMonth", F.month("InvoiceDate")) \
            .withColumn("SalesDayOfWeek", F.dayofweek("InvoiceDate"))

        return df


class DataCleaningTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, uid=None):
        super(DataCleaningTransformer, self).__init__()
        self.uid = uid or "DataCleaningTransformer_" + str(uuid.uuid4())

    def _transform(self, df):
        df = correct_data_types(df)
        df = special_stock_codes(df)
        df = fill_customer_id(df)
        df = fill_country(df)
        df = fill_invoice_date(df)
        df = fill_unit_price(df)
        df = clean_descriptions(df)
        df = fill_description(df)
        df = fill_invoice_no(df)
        df = handle_correction_invoices(df)


        return df

def create_pipeline():
    data_cleaning = DataCleaningTransformer()
    feature_engineering = FeatureEngineeringTransformer()
    pipeline = Pipeline(stages=[data_cleaning, feature_engineering])
    return pipeline

def run_pipeline(pipeline, df):
    return pipeline.fit(df).transform(df)

def rfm_pipeline(df):
    product_sales_result = product_sales(df)

    rfm_result = rfm(df)
    rfm_segmented = rfm_segments(rfm_result)

    return rfm_segmented

def get_new_data(last_processed_date):
    new_data = spark.read.format("bigquery") \
        .option("credentialsFile", credentials_abs_path) \
        .option("parentProject", "ecommerce-analysis-453419") \
        .option("materializationDataset", "online_retail_27") \
        .option("dataset", "online_retail_27") \
        .option("viewsEnabled", "true") \
        .option("query", f"""
            SELECT * FROM `ecommerce-analysis-453419.online_retail_27.online_retail_27_raw`
            WHERE InvoiceDate > '{last_processed_date}'
        """) \
        .load()
    return new_data