using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;

namespace QueryFunctionalitySamples
{
    public class QueryRunner
    {
        private Container container;

        public bool AccountWithCompositeIndexes { get; set; }

        public QueryRunner(CosmosClient client, string database, string containerName)
        {
            container = client.GetDatabase(database).GetContainer(containerName);
        }

        public async Task<QueryStats> RunQueryAsync(string queryText)
        {
            var query = new QueryDefinition(queryText);

            PrintQuerySetup(queryText);

            var requestCharge = 0.0;
            var executionTime = new TimeSpan();
            var results = new List<dynamic>();
            var indexMetrics = "";

            var options = new QueryRequestOptions()
            {
                PopulateIndexMetrics = true,
                MaxConcurrency = -1,
                MaxItemCount = -1,
                MaxBufferedItemCount = -1
            };
            var resultSetIterator = container.GetItemQueryIterator<dynamic>(query, null, options);
            while (resultSetIterator.HasMoreResults)
            {
                var response = await resultSetIterator.ReadNextAsync();
                results.AddRange(response.Resource);
                requestCharge += response.RequestCharge;
                executionTime += response.Diagnostics.GetClientElapsedTime();

                //Console.WriteLine($"Trip num items: {response.Count}, Trip request charge: {response.RequestCharge}, Trip execution time: {response.Diagnostics.GetClientElapsedTime()}");

                if (indexMetrics == "")
                    indexMetrics = response.IndexMetrics;
            }

            Console.WriteLine(indexMetrics);
            Console.WriteLine($"Final Request charge: {requestCharge}, Final execution time: {executionTime}\n\n");

            var stats = new QueryStats()
            {
                RUCharge = requestCharge,
                ExecutionTime = executionTime
            };

            return stats;
        }

        public void PrintQuerySetup(string queryText)
        {
            if (AccountWithCompositeIndexes)
            {
                Console.WriteLine($"Running against container {container.Id} with composite indexes.");
            }
            else
            {
                Console.WriteLine($"Running against container {container.Id} without composite indexes.");
            }
            Console.WriteLine($"\t* Query: {queryText}\n");
        }

        public async void SetUp()
        {
            // Read the current container properties
            var containerProperties = await container.ReadContainerAsync();

            // Add a new computed property
            containerProperties.Resource.ComputedProperties = new Collection<ComputedProperty>
            {
                new ComputedProperty
                {
                    Name = "cp_NameContainsIncredible",
                    Query = "SELECT VALUE CONTAINS(c.Name, \"Incredible\") FROM c"
                },
                new ComputedProperty
                {
                    Name = "cp_LowerName",
                    Query = "SELECT VALUE LOWER(c.Name) FROM c"
                }
            };

            // Add the computed property to the indexing policy
            containerProperties.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath() { Path = "/cp_NameContainsIncredible/?" });
            containerProperties.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath() { Path = "/cp_LowerName/?" });

            // Add all composite indexes to the indexing policy
            containerProperties.Resource.IndexingPolicy.CompositeIndexes
                .Add(new Collection<CompositePath> { new CompositePath() { Path = "/FirstAvailable", Order = CompositePathSortOrder.Ascending }, new CompositePath() { Path = "/Name", Order = CompositePathSortOrder.Descending } });
            containerProperties.Resource.IndexingPolicy.CompositeIndexes
                .Add(new Collection<CompositePath> { new CompositePath() { Path = "/_ts", Order = CompositePathSortOrder.Descending }, new CompositePath() { Path = "/Name", Order = CompositePathSortOrder.Descending } });
            containerProperties.Resource.IndexingPolicy.CompositeIndexes
                .Add(new Collection<CompositePath> { new CompositePath() { Path = "/Category", Order = CompositePathSortOrder.Ascending }, new CompositePath() { Path = "/FirstAvailable", Order = CompositePathSortOrder.Ascending } });
            containerProperties.Resource.IndexingPolicy.CompositeIndexes
                .Add(new Collection<CompositePath> { new CompositePath() { Path = "/Category", Order = CompositePathSortOrder.Ascending }, new CompositePath() { Path = "/Name", Order = CompositePathSortOrder.Ascending }, new CompositePath() { Path = "/_ts", Order = CompositePathSortOrder.Ascending } });
            containerProperties.Resource.IndexingPolicy.CompositeIndexes
                .Add(new Collection<CompositePath> { new CompositePath() { Path = "/Category", Order = CompositePathSortOrder.Ascending }, new CompositePath() { Path = "_ts", Order = CompositePathSortOrder.Ascending } });
            containerProperties.Resource.IndexingPolicy.CompositeIndexes
                .Add(new Collection<CompositePath> { new CompositePath() { Path = "/Category", Order = CompositePathSortOrder.Ascending }, new CompositePath() { Path = "Price", Order = CompositePathSortOrder.Ascending } });

            // Update the container with changes
            await container.ReplaceContainerAsync(containerProperties);
        }
    }
}
