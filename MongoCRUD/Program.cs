using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;

namespace MongoCRUD
{
    class Program
    {
        static void Main(string[] args)
        {
            IMongoCollection<BsonDocument> collection = GetCollection();
            var query = new QueryModel();
            query.PrimaryFields.Add(new Field { FieldName = "Region" });
            query.SecondaryFields.Add(new Field { FieldName = "TotalCost", Measure = "sum" });
            query.SecondaryFields.Add(new Field { FieldName = "TotalProfit", Measure = "avg" });
            query.Filters.Add(new Filter
            {
                FieldName = "Region",
                FieldValue = new List<object> { "Sub-Saharan Africa", "Europe", "Middle East and North Africa", "Central America and the Caribbean", "Asia" },
                Operation = "in"
            });
            query.SortBy.Add(new Field { FieldName = "Region", OrderBy = 1 });
            var result = GetData(query);
            foreach (var example in result)
            {
                Console.WriteLine(example);
            }
        }

        public static dynamic GetData(QueryModel query)
        {
            var collection = GetCollection();
            BsonDocument matchBd = GetMatchFilters(query);
            BsonDocument groupBd = GetGroupByFields(query);
            BsonDocument sortBd = GetSortBy(query);
            BsonDocument projectBd = GetProjection(query);

            var pipeline = new[] { matchBd, groupBd, projectBd, sortBd };
            var aggregateOptions = new AggregateOptions { AllowDiskUse = true };
            var result = collection.Aggregate<BsonDocument>(pipeline, aggregateOptions).ToList();


            return result;
        }

        private static BsonDocument GetProjection(QueryModel query)
        {
            BsonDocument projectionFields = new BsonDocument { { "_id", 0 } };
            foreach (var pf in query.PrimaryFields)
            {
                projectionFields.Add(pf.FieldName, "$_id." + pf.FieldName);
            }
            foreach (var sf in query.SecondaryFields)
            {
                projectionFields.Add(sf.FieldName, 1);
            }
            BsonDocument projectBd = new BsonDocument("$project", projectionFields);
            return projectBd;
        }

        private static BsonDocument GetSortBy(QueryModel query)
        {
            BsonDocument sortBd = new BsonDocument();
            BsonDocument sortElements = new BsonDocument();
            foreach (var f in query.SortBy.OrderBy(s => s.Priority))
            {
                sortElements.Add(f.FieldName, f.OrderBy);
            }
            sortBd.Add("$sort", sortElements);
            return sortBd;
        }

        private static BsonDocument GetGroupByFields(QueryModel query)
        {
            BsonDocument groupFields = new BsonDocument();
            BsonDocument idBd = new BsonDocument();
            foreach (var pf in query.PrimaryFields)
            {
                idBd.Add(new BsonElement(pf.FieldName, "$" + pf.FieldName));
            }
            groupFields.Add(new BsonElement("_id", idBd));
            foreach (var sf in query.SecondaryFields)
            {
                groupFields.Add(new BsonElement(sf.FieldName, new BsonDocument("$" + sf.Measure, "$" + sf.FieldName)));
            }
            var groupBd = new BsonDocument("$group", groupFields);
            return groupBd;
        }

        private static BsonDocument GetMatchFilters(QueryModel query)
        {
            BsonDocument matchBd = new BsonDocument();
            BsonDocument matchFilters = new BsonDocument();
            for (int i = 0; i < query.Filters.Count; i++)
            {
                var filter = query.Filters[i];

                switch (filter.Operation)
                {
                    case ">":
                        matchFilters.Add(filter.FieldName,
                            new BsonDocument { { filter.FieldName, new BsonDocument(new BsonElement("$gt", (BsonValue)filter.FieldValue.FirstOrDefault())) } });
                        break;
                    case ">=":
                        matchFilters.Add(filter.FieldName,
                            new BsonDocument { { filter.FieldName, new BsonDocument(new BsonElement("$gte", (BsonValue)filter.FieldValue.FirstOrDefault())) } });
                        break;
                    case "<":
                        matchFilters.Add(filter.FieldName,
                            new BsonDocument { { filter.FieldName, new BsonDocument(new BsonElement("$lt", (BsonValue)filter.FieldValue.FirstOrDefault())) } });
                        break;
                    case "<=":
                        matchFilters.Add(filter.FieldName,
                            new BsonDocument { { filter.FieldName, new BsonDocument(new BsonElement("$lte", (BsonValue)filter.FieldValue.FirstOrDefault())) } });
                        break;
                    case "in":
                        matchFilters.Add(new BsonElement(filter.FieldName, new BsonDocument { { "$in", new BsonArray(
                            filter.FieldValue
                            )}}));
                        break;
                    case "like":
                        break;
                    case "sw":
                        break;
                    case "ew":
                        break;
                    default:
                        matchFilters.Add(new BsonElement(filter.FieldName, (BsonValue)filter.FieldValue.FirstOrDefault()));
                        break;
                }
            }
            matchBd.Add("$match", matchFilters);
            return matchBd;
        }

        private static void AggregateExample2(IMongoCollection<BsonDocument> collection)
        {
            var agg = collection.Aggregate()
                .Group(
                new BsonDocument {
                    {"_id",new BsonDocument {{ "bedtype", "$bed_type"},{ "roomtype", "$room_type" } } },
                    {"totalReviews", new BsonDocument("$sum", "$number_of_reviews")},
                    {"totalBeds", new BsonDocument ("$sum", "$beds")}
                })
                .Sort(new BsonDocument { { "_id.roomtype", 1 }, { "totalBeds", -1 } }).ToList();
        }

        private static void CreateView(BsonDocument[] pipeline)
        {
            var db = GetDatabase();
            db.CreateView<BsonDocument, BsonDocument>("rgo", "sales_data", pipeline);
        }

        private static void ImportData(IMongoCollection<BsonDocument> collection)
        {
            var jsonData = ConvertCsvFileToJsonObject(@"c:/10K Sales Records.csv");
            var BSONDoc = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<List<BsonDocument>>(jsonData);
            collection.InsertMany(BSONDoc);
        }

        private static IMongoCollection<BsonDocument> GetCollection()
        {
            IMongoDatabase database = GetDatabase();
            var collection = database.GetCollection<BsonDocument>("sales_data");
            return collection;
        }

        private static IMongoDatabase GetDatabase()
        {
            var client = new MongoClient("mongodb+srv://syed:xxx@cluster0.vwpbm.mongodb.net/mongo_test? retryWrites =true&w=majority");
            var database = client.GetDatabase("mongo_test");
            return database;
        }

        private static void BucketAuto(IMongoCollection<BsonDocument> collection)
        {
            var groupBy = (AggregateExpressionDefinition<BsonDocument, BsonValue>)"$Region";
            var boundaries = new BsonValue[] { 1900, 1920, 1950 };
            var output = (ProjectionDefinition<BsonDocument, BsonDocument>)"{ Regions : { $push : \"$Region\" }, count : { $sum : 1 } }";
            var autoBucket = collection.Aggregate().BucketAuto(groupBy, 1);
            var autoBucketResults = autoBucket.ToList();
        }

        public static string ConvertCsvFileToJsonObject(string path)
        {
            var csv = new List<string[]>();
            var lines = File.ReadAllLines(path);
            Console.WriteLine("Lines count csv: " + lines.Length);
            foreach (string line in lines)
                csv.Add(line.Split(','));

            var properties = lines[0].Split(',');
            var propMeta = generateMeta(csv, properties);

            var listObjResult = new List<Dictionary<string, BsonValue>>();

            for (int i = 1; i < lines.Length; i++)
            {
                var objResult = new Dictionary<string, BsonValue>();
                for (int j = 0; j < properties.Length; j++)
                {
                    var key = properties[j].Replace(" ", "").Trim();
                    dynamic val = csv[i][j];

                    try
                    {
                        switch (propMeta[key])
                        {
                            case "Int32":
                            case "Single":
                                try
                                {
                                    objResult[key] = new BsonInt32(Int32.Parse(val));
                                }
                                catch (Exception)
                                {
                                    objResult[key] = new BsonInt64(Int64.Parse(val));
                                }
                                break;
                            case "Double":
                                objResult[key] = new BsonDouble(Double.Parse(val));
                                break;
                            case "DateTime":
                                CultureInfo culture = new CultureInfo("en-US");
                                objResult[key] = new BsonDateTime(DateTime.Parse(val, culture));

                                break;
                            case "Boolean":
                                objResult[key] = new BsonBoolean(Boolean.Parse(val));
                                break;
                            default:
                                objResult[key] = new BsonString(val);
                                break;
                        }
                    }
                    catch (Exception)
                    {
                        objResult[key] = new BsonString(val);
                    }
                }
                listObjResult.Add(objResult);
            }
            Console.WriteLine("Object to serialize count: " + listObjResult.Count);
            return Newtonsoft.Json.JsonConvert.SerializeObject(listObjResult);
        }

        private static Dictionary<string, string> generateMeta(List<string[]> csv, string[] properties)
        {
            var rand = new Random();
            var maxRows = csv.Count;
            var rowsToTest = 6;
            var testRes = new Dictionary<string, List<string>>();
            foreach (var prop in properties)
            {
                var propt = prop.Replace(" ", "").Trim();
                testRes.Add(propt, new List<string>());
            }
            for (int counter = 0; counter < rowsToTest; counter++)
            {
                var rowNum = rand.Next(1, maxRows);
                for (int prop = 0; prop < properties.Length; prop++)
                {
                    var key = properties[prop].Replace(" ", "").Trim();
                    dynamic val = csv[rowNum][prop];
                    var tt = GetValType(val);
                    testRes[key].Add(tt);
                }
            }

            var res = new Dictionary<string, string>();
            foreach (var item in testRes)
            {
                string maxRepeated = item.Value.GroupBy(s => s)
                         .OrderByDescending(s => s.Count())
                         .First().Key;
                res.Add(item.Key, maxRepeated);
            }
            return res;
        }

        public static object DetectType(string stringValue)
        {
            var expectedTypes = new List<Type> { typeof(DateTime), typeof(float), typeof(long), typeof(int), typeof(bool) };
            foreach (var type in expectedTypes)
            {
                TypeConverter converter = TypeDescriptor.GetConverter(type);
                if (converter.CanConvertFrom(typeof(string)))
                {
                    try
                    {
                        // You'll have to think about localization here
                        object newValue = converter.ConvertFromInvariantString(stringValue);
                        if (newValue != null)
                        {
                            return newValue;
                        }
                    }
                    catch
                    {
                        // Can't convert given string to this type
                        continue;
                    }

                }
            }

            return stringValue;
        }

        public static string GetValType(string stringValue)
        {
            var expectedTypes = new List<Type> { typeof(DateTime), typeof(int), typeof(long), typeof(Double), typeof(bool) };
            foreach (var type in expectedTypes)
            {
                TypeConverter converter = TypeDescriptor.GetConverter(type);
                if (converter.CanConvertFrom(typeof(string)))
                {
                    try
                    {
                        object newValue = converter.ConvertFromInvariantString(stringValue);
                        if (newValue != null)
                        {
                            return newValue.GetType().Name;
                        }
                    }
                    catch
                    {
                        continue;
                    }
                }
            }
            return typeof(string).Name;
        }
    }
}
