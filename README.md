# Azure Cosmos DB Query Demo

This application shows how adding composite indexes to a container can improve cost and performance of Azure Cosmos DB queries.

## Setup

Rename the `appsettings.sample.json` file to `appsettings.json`, fill out the values, and run the application in Visual Studio with `CTRL + F5` or with the command line with `dotnet run`.

> Note: this application requires two containers to already be created in your Azure Cosmos DB account. If you set `SetupDemoResources` to `true` in the appsettings file, a computed property and all necessary composite indexes will be created for you on the container specified in `ContainerWithComposites`. Re-indexing can take some time to complete. Please wait for re-indexing to finish on your container before running the rest of the demo.

### Ingest sample data

Load the exact same data into both your container with and without composite indexes to create the best comparison for queries. Sample data for this application can be generated using [Bogus](https://github.com/bchavez/Bogus) and should have the following shape:

```json
{
    "id": "3a805482-cbfe-45f6-8fef-d4fce30438c4",
    "Name": "Handcrafted Steel Fish",
    "Price": 192.22,
    "Category": "Outdoors",
    "Description": "The Football Is Good For Training And Recreational Purposes",
    "FirstAvailable": "2021-12-25T10:48:43.4123257Z",
    "CustomerRatings": [
        {
            "Username": "Chadrick17",
            "Stars": 4
        },
        {
            "Username": "Antonetta24",
            "Stars": 2
        },
        {
            "Username": "Jodie.Beier",
            "Stars": 3
        },
        {
            "Username": "Giuseppe50",
            "Stars": 2
        },
        {
            "Username": "Amina_Tillman78",
            "Stars": 3
        }
    ]
}
```
