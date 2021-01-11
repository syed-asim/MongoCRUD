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

            // >> Data Insert >>>
            // ImportData(collection);

            // >> Bucket Auto Example
            //BucketAuto(collection);

            // >> Method 1 
            var group = new BsonDocument(
                 "$group", new BsonDocument {
                    {"_id", new BsonDocument {{ "Region", "$Region" },{ "Country", "$Country" } }},
                    {"TotalProfit", new BsonDocument("$sum", "$TotalProfit")},
                    {"TotalCost", new BsonDocument("$sum", "$TotalCost")},
                 });



            var match3 = new BsonDocument
                {
                    {
                        "$match",
                        new BsonDocument
                            {
                                {"Count", new BsonDocument
                                                   {
                                                       {
                                                           "$gte", 2
                                                       }
                                                   }}
                            }
                    }
                };

            var match2 = new BsonDocument
                        {
                            {
                                "$match",
                                new BsonDocument
                                {
                                    { "Region", new BsonDocument{
                                        {  "$in",
                                        new BsonArray {
                                            "Sub-Saharan Africa" ,
                                            "Europe"
                                        } }
                                        } }
                                }
                            }
                        };


            var sort = new BsonDocument("$sort", new BsonDocument { { "TotalProfit", -1 }, /*{ "totalBeds", -1 }*/ });
            var pipeline = new[] { match2,/*project,*/ group, sort };

            var db = GetDatabase();
            var match = new BsonDocument
                        {
                            {
                                "$match",
                                new BsonDocument
                                {
                                    {"Region", "Asia"}
                                }
                            }
                        };
            var proj = new BsonDocument
                        {
                            {
                                "$project",
                                new BsonDocument
                                {
                                    {"_id", 0},
                                    {"Region","$_id.Region"},
                                    {"Country","$_id.Country"},
                                    {"TotalProfit",1 },
                                    {"TotalCost",1 }
                                }
                            }
                        };
            var pipelineForView = new BsonDocument[] { match2,/*project,*/ group, sort, proj };
            db.CreateView<BsonDocument, BsonDocument>("rgo", "sales_data", pipelineForView);
            var vw = db.GetCollection<BsonDocument>("rgo");
            var rs = vw.CountDocuments(new BsonDocument { });



            var aggregateOptions = new AggregateOptions { AllowDiskUse = true };
            var result = collection.Aggregate<BsonDocument>(pipeline, aggregateOptions).ToList();


            // Method 2 >>>

            //var agg = collection.Aggregate()
            //    .Group(
            //    new BsonDocument {
            //        {"_id",new BsonDocument {{ "bedtype", "$bed_type"},{ "roomtype", "$room_type" } } },
            //        {"totalReviews", new BsonDocument("$sum", "$number_of_reviews")},
            //        {"totalBeds", new BsonDocument ("$sum", "$beds")}
            //    })
            //    .Sort(new BsonDocument { { "_id.roomtype", 1 }, { "totalBeds", -1 } }).ToList();


            foreach (var example in result)
            {
                Console.WriteLine(example);
            }
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
            var client = new MongoClient("mongodb+srv://syed:qwer1234@cluster0.vwpbm.mongodb.net/mongo_test? retryWrites =true&w=majority");
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
