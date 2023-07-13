using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace QueryFunctionalitySamples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            var client = new CosmosClient(configuration["Endpoint"], configuration["Key"]);
            var database = configuration["Database"];
            var containerWith = configuration["ContainerWithComposites"];
            var containerWithout = configuration["ContainerWithoutComposites"];
            var setup = bool.Parse(configuration["SetupDemoResources"]);

            var runnerWithoutComposites = new QueryRunner(client, database, containerWithout)
            {
                AccountWithCompositeIndexes = false
            };
            var runnerWithComposites = new QueryRunner(client, database, containerWith)
            {
                AccountWithCompositeIndexes = true
            };

            if (setup)
            {
                runnerWithComposites.SetUp();
            }

            await RunDemoMenu(runnerWithComposites, runnerWithoutComposites);
        }

        public static async Task RunDemoMenu(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            bool exit = false;

            while (exit == false)
            {
                Console.Clear();
                Console.WriteLine($"Azure Cosmos DB Query Demo");
                Console.WriteLine($"-----------------------------------------------------------");
                Console.WriteLine($"[1]   Cross partition query");
                Console.WriteLine($"[2]   Mutiple ORDER BY properties");
                Console.WriteLine($"[3]   Complex filter expressions");
                Console.WriteLine($"[4]   Aggregations");
                Console.WriteLine($"[5]   Multiple GROUP BY properties");
                Console.WriteLine($"[6]   Computed properties");
                Console.WriteLine($"[7]   EXISTS queries");
                Console.WriteLine($"[8]   UDFs vs system functions");
                Console.WriteLine($"[9]   Exit\n");

                ConsoleKeyInfo result = Console.ReadKey(true);

                if (result.KeyChar == '1')
                {
                    await RunXPartDemo(runnerWith);
                }
                else if (result.KeyChar == '2')
                {
                    await RunOrderByDemo(runnerWith, runnerWithout);
                }
                if (result.KeyChar == '3')
                {
                    await RunComplexFilterDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '4')
                {
                    await RunAggregationsDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '5')
                {
                    await RunGroupByDemo(runnerWith, runnerWithout);
                }
                else if (result.KeyChar == '6')
                {
                    await RunComputedPropertiesDemo(runnerWith);
                }
                else if (result.KeyChar == '7')
                {
                    await RunExistsDemo(runnerWith);
                }
                else if (result.KeyChar == '8')
                {
                    await RunUDFDemo(runnerWith);
                }
                else if (result.KeyChar == '9')
                {
                    Console.WriteLine("Goodbye!");
                    exit = true;
                }
            }
        }

        public static async Task RunXPartDemo(QueryRunner runner)
        {
            Console.WriteLine("Showing cross partition query sdk options.");
            Console.WriteLine($"-----------------------------------------------------------");

            var xPartText = "SELECT * \n\t\tFROM c \n\t\tWHERE c.Category = \"Movies\"";

            var stats = await runner.RunQueryAsync(xPartText);

            PrintComparisonOutput(stats, null, xPartText);
        }

        public static async Task RunOrderByDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing ORDER BY multiple properties.");
            Console.WriteLine($"-----------------------------------------------------------");

            var orderByText0 = "SELECT TOP 1000 c.Price, c.FirstAvailable, c.Name \n\t\tFROM c \n\t\tORDER BY c.FirstAvailable DESC, c.Name DESC";

            // Running this query on a container without composite indexes will fail because ORDER BY must be served from the index
            // var statsWithout0 = await runnerWithout.RunQueryAsync(orderByText0);
            var statsWith0 = await runnerWith.RunQueryAsync(orderByText0);

            PrintComparisonOutput(statsWith0, null, orderByText0);

            //var orderByText = "SELECT c.Price, c.FirstAvailable, c.Name \n\t\tFROM c \n\t\tWHERE c.Category = \"Electronics\" AND STARTSWITH(c.Name, \"Sleek\", false) \n\t\tORDER BY c.FirstAvailable DESC, c.Name DESC";

            //var statsWith = await runnerWith.RunQueryAsync(orderByText);

            //PrintComparisonOutput(statsWith, null, orderByText);
        }

        public static async Task RunComplexFilterDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing queries with complex filters.");
            Console.WriteLine($"-----------------------------------------------------------");

            var complexFilterText_without = "SELECT c.Name, r.Stars as Rating \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE c.Category = \"Tools\" and c.Name = \"Incredible Soft Cheese\" and c._ts > 1655444336";
            var complexFilterText_with = "SELECT c.Name, r.Stars as Rating \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE c.Category = \"Tools\" and c.Name = \"Incredible Soft Cheese\" and c._ts > 1676270097";

            var statsWithout = await runnerWithout.RunQueryAsync(complexFilterText_without);
            var statsWith = await runnerWith.RunQueryAsync(complexFilterText_with);

            PrintComparisonOutput(statsWith, statsWithout, complexFilterText_with);

            var complexFilterText2_without = "SELECT c.Name, r.Stars as Rating \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE c.Category = \"Tools\" and CONTAINS(c.Name, \"Incredible\") and c._ts > 1655444336";
            var complexFilterText2_with = "SELECT c.Name, r.Stars as Rating \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE c.Category = \"Tools\" and CONTAINS(c.Name, \"Incredible\") and c._ts > 1676270097";

            var statsWithout2 = await runnerWithout.RunQueryAsync(complexFilterText2_without);
            var statsWith2 = await runnerWith.RunQueryAsync(complexFilterText2_with);

            PrintComparisonOutput(statsWith2, statsWithout2, complexFilterText2_with);
        }

        public static async Task RunAggregationsDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing aggregations with filters.");
            Console.WriteLine($"-----------------------------------------------------------");

            var aggregationsText = "SELECT AVG(c.Price) as AveragePrice\n\t\tFROM c \n\t\tWHERE c.Category = \"Outdoors\"";

            var statsWithout = await runnerWithout.RunQueryAsync(aggregationsText);
            var statsWith = await runnerWith.RunQueryAsync(aggregationsText);

            PrintComparisonOutput(statsWith, statsWithout, aggregationsText);

            var aggregationsText2_without = "SELECT Count(1) as NumProducts, AVG(c.Price) as AveragePrice \n\t\tFROM c \n\t\tWHERE c.Category = \"Outdoors\" and c.Price > 500 and c._ts > 1655444336";
            var aggregationsText2_with = "SELECT Count(1) as NumProducts, AVG(c.Price) as AveragePrice \n\t\tFROM c \n\t\tWHERE c.Category = \"Outdoors\" and c.Price > 500 and c._ts > 1676270097";

            var statsWithout2 = await runnerWithout.RunQueryAsync(aggregationsText2_without);
            var statsWith2 = await runnerWith.RunQueryAsync(aggregationsText2_with);

            PrintComparisonOutput(statsWith2, statsWithout2, aggregationsText2_with);
        }

        public static async Task RunGroupByDemo(QueryRunner runnerWith, QueryRunner runnerWithout)
        {
            Console.WriteLine("Showing GROUP BY multiple properties.");
            Console.WriteLine($"-----------------------------------------------------------");

            //var groupByText = "SELECT Count(1) as NewProduts, c.Category, DateTimeBin(c.FirstAvailable,'d', 30, \"2021-01-01T00:00:00.0000000Z\") AS DayAvailable \n\t\tFROM c \n\t\tWHERE c.FirstAvailable > \"2021-01-01T00:00:00.0000000Z\" \n\t\tGROUP BY c.Category, DateTimeBin(c.FirstAvailable,'d',30, \"2021-01-01T00:00:00.0000000Z\")";
            var groupByText = "SELECT Count(1) as NewProduts, c.Category, DateTimeBin(c.FirstAvailable,'d', 30, \"2022-01-01T00:00:00.0000000Z\") \n\t\tFROM c \n\t\tWHERE c.FirstAvailable > \"2022-01-01T00:00:00.0000000Z\" \n\t\tGROUP BY c.Category, DateTimeBin(c.FirstAvailable,'d', 30, \"2022-01-01T00:00:00.0000000Z\")";

            var statsWithout = await runnerWithout.RunQueryAsync(groupByText);
            var statsWith = await runnerWith.RunQueryAsync(groupByText);

            PrintComparisonOutput(statsWith, statsWithout, groupByText);
        }

        public static async Task RunComputedPropertiesDemo(QueryRunner runner)
        {
            Console.WriteLine("Showing query improvements by using computed properties.");
            Console.WriteLine($"-----------------------------------------------------------");

            var withoutComputedPropsText = "SELECT c.Name, r.Stars as Rating \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE c.Category = \"Tools\" and CONTAINS(c.Name, \"Incredible\")";
            var withComputedPropsText = "SELECT c.Name, r.Stars as Rating \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE c.Category = \"Tools\" and c.cp_NameContainsIncredible";

            var statsWithout = await runner.RunQueryAsync(withoutComputedPropsText);
            var statsWith = await runner.RunQueryAsync(withComputedPropsText);

            PrintComparisonOutput(statsWith, statsWithout);
        }

        public static async Task RunExistsDemo(QueryRunner runner)
        {
            Console.WriteLine("Showing query improvements by using EXISTS.");
            Console.WriteLine($"-----------------------------------------------------------");

            var arrayContainsText2 = "SELECT COUNT(1) as FiveStarProducts \n\t\tFROM c \n\t\tWHERE ARRAY_CONTAINS(c.CustomerRatings, {\"Stars\": 5}, true)";
            var existsText2 = "SELECT COUNT(1) as FiveStarProducts \n\t\tFROM c \n\t\tWHERE EXISTS(SELECT VALUE 1 FROM rating IN c.CustomerRatings WHERE rating.Stars = 5)";

            var statsWithout2 = await runner.RunQueryAsync(arrayContainsText2);
            var statsWith2 = await runner.RunQueryAsync(existsText2);

            PrintComparisonOutput(statsWith2, statsWithout2);

            //var joinText3 = "SELECT COUNT(1) as FourPlusStarProductsFromCaroline \n\t\tFROM c \n\t\tJOIN r IN c.CustomerRatings \n\t\tWHERE r.Stars > 4 and r.UserName = \"Caroline80\"";
            //var existsText3 = "SELECT COUNT(1) as FourPlusStarProductsFromCaroline \n\t\tFROM c \n\t\tWHERE EXISTS(SELECT VALUE 1 FROM rating IN c.CustomerRatings WHERE rating.Stars > 4 and rating.UserName = \"Caroline80\")";

            //var statsWithout3 = await runner.RunQueryAsync(joinText3);
            //var statsWith3 = await runner.RunQueryAsync(existsText3);

            //PrintComparisonOutput(statsWith3, statsWithout3);
        }

        public static async Task RunUDFDemo(QueryRunner runner)
        {
            Console.WriteLine("Showing UDFs vs system functions.");
            Console.WriteLine($"-----------------------------------------------------------");

            var udfText = "SELECT TOP 1000 c.Name, udf.OneMonthLater(c.FirstAvailable) as OneMonthLater \n\t\tFROM c \n\t\tWHERE c.FirstAvailable > \"2022-01-01T00:00:00.0Z\"";
            var sysFuncText = "SELECT TOP 1000 c.Name, DateTimeAdd(\"mm\", 1, c.FirstAvailable) AS OneMonthLater \n\t\tFROM c \n\t\tWHERE c.FirstAvailable > \"2022-01-01T00:00:00.0Z\"";

            var statsWithout = await runner.RunQueryAsync(udfText);
            var statsWith = await runner.RunQueryAsync(sysFuncText);

            PrintComparisonOutput(statsWith, statsWithout);

            //var udfText2 = "SELECT TOP 1000 c.Name, udf.OneMonthLater(c.FirstAvailable) \n\t\tFROM c \n\t\tWHERE udf.OneMonthLater(c.FirstAvailable) > \"2022-01-01T00:00:00.0Z\"";
            //var subqueryText = "SELECT TOP 1000 c.Name, OneMonthLater \n\t\tFROM c \n\t\tJOIN (SELECT VALUE udf.OneMonthLater(c.FirstAvailable)) OneMonthLater \n\t\tWHERE OneMonthLater > \"2022-01-01T00:00:00.0Z\"";

            //var statsWithout2 = await runner.RunQueryAsync(udfText2);
            //var statsWith2 = await runner.RunQueryAsync(subqueryText);

            //PrintComparisonOutput(statsWith2, statsWithout2);
        }

        private static void PrintComparisonOutput(QueryStats statsWith, QueryStats statsWithout, string queryText)
        {
            Console.WriteLine($"\nShowing final results for query \"{queryText}\"");
            Console.WriteLine($"-----------------------------------------------------------");

            Console.WriteLine("|Account         |RU Charge |Execution Time  |");
            Console.WriteLine("|----------------|----------|----------------|");
            if (statsWithout != null)
            {
                Console.WriteLine("|Without indexes |{0, -10}|{1, -16}|", Math.Round(statsWithout.RUCharge, 2), statsWithout.ExecutionTime);
            }
            Console.WriteLine("|With indexes    |{0, -10}|{1, -16}|", Math.Round(statsWith.RUCharge, 2), statsWith.ExecutionTime);

            Console.WriteLine("Press enter to continue...");
            Console.ReadLine();
            Console.WriteLine();
        }

        private static void PrintComparisonOutput(QueryStats statsWith, QueryStats statsWithout)
        {
            Console.WriteLine($"\nShowing final results for query with and without improvements");
            Console.WriteLine($"-----------------------------------------------------------");

            Console.WriteLine("|Query                |RU Charge |Execution Time  |");
            Console.WriteLine("|---------------------|----------|----------------|");
            Console.WriteLine("|Without improvements |{0, -10}|{1, -16}|", Math.Round(statsWithout.RUCharge, 2), statsWithout.ExecutionTime);
            Console.WriteLine("|With improvements    |{0, -10}|{1, -16}|", Math.Round(statsWith.RUCharge, 2), statsWith.ExecutionTime);

            Console.WriteLine("Press enter to continue...");
            Console.ReadLine();
            Console.WriteLine();
        }
    }
}
