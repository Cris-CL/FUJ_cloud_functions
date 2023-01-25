/**
 * Loads a sheet into BigQuery
 */
function load_report_ss() {
  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  const projectId = 'test-bigquery-cc';
  // Create a dataset in the BigQuery UI (https://bigquery.cloud.google.com)
  // and enter its ID below.
  const datasetId = 'apps_script';
  var name = "100937728312_01_18";  /////// last updated settlement id number in bigquery

  // Create the table.
  const tableId = name;
  let table = {
    tableReference: {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId
    },
    schema: {
      fields: [
        {name: 'balance', type: 'STRING'},
        {name: 'settlement_id', type: 'STRING'},
        {name: 'purchase_or_posted_date', type: 'DATETIME'},
        {name: 'payment_date', type: 'DATETIME'},
        {name: 'supplier', type: 'STRING'},
        {name: 'account', type: 'STRING'},
        {name: 'tax_distiction', type: 'STRING'},
        {name: 'total_sum', type: 'FLOAT64'},
        {name: 'tax_calc_distinction', type: 'STRING'},
        {name: 'tax_amount', type: 'STRING'},

        {name: 'remarks', type: 'STRING'},
        {name: 'item', type: 'STRING'},
        {name: 'department', type: 'STRING'},
        {name: 'memo', type: 'STRING'},
        {name: 'pay_date', type: 'DATETIME'},

        {name: 'payment_account', type: 'STRING'},
        {name: 'payment_amount', type: 'FLOAT64'},
        {name: 'TOTAL', type: 'FLOAT64'},

        // {name: 'order_item_code', type: 'STRING'},
        // {name: 'merchant_order_item_id', type: 'STRING'},
        // {name: 'merchant_adjustment_item_id', type: 'STRING'},
        // {name: 'sku', type: 'STRING'},
        // {name: 'quantity_purchased', type: 'FLOAT64'},
        // {name: 'promotion_id', type: 'STRING'},

      ]
    }
  };
  try {
    table = BigQuery.Tables.insert(table, projectId, datasetId);
    Logger.log('Table created: %s', table.id);
  } catch (err) {
    Logger.log('unable to create table');
  }
  // Load CSV data from Drive and convert to the correct format for upload.

  var file = SpreadsheetApp.getActiveSpreadsheet();
  var currentSheet = file.getSheetByName(name);
  Logger.log(currentSheet);
  Logger.log(name);
  var lastRow = currentSheet.getLastRow();
  var lastC = currentSheet.getLastColumn();
  var rows = currentSheet.getRange(1,1,lastRow,lastC).getValues();
  var rowsCSV = rows.join("\n");
  // Logger.log("Check This"+" "+rowsCSV);
  var data = Utilities.newBlob(rowsCSV, 'application/octet-stream');


  // Create the data upload job.
  const job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        skipLeadingRows: 1
      }
    }
  };
  try {
    BigQuery.Jobs.insert(job, projectId, data);
    Logger.log('Load job started. Check on the status of it here: ' +
      'https://bigquery.cloud.google.com/jobs/%s', projectId);
  } catch (err) {
    Logger.log('unable to insert job');
  }
}
