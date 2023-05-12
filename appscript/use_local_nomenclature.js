/**
 * Loads a sheet into BigQuery
 */
function use_local_nomenclature22() {
  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  const projectId = 'test-bigquery-cc';
  // Create a dataset in the BigQuery UI (https://bigquery.cloud.google.com)
  // and enter its ID below.
  const datasetId = 'temp_nomenclature';

  // Amazon source data
  const Amazon_datasetId = 'Amazon';
  const order_central_tableId = 'order_central_master';
  const transaction_view_tableId = 'transaction_view_master';

  // name is the name of the sheet where the nomenclature is stored
  var name = "Nomenclature";
  // Create the table.
  const tableId = 'nomenclature_temp';

  let table = {
    tableReference: {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId
    },
    schema: {
      fields: [
        {name: 'transaction_type', type: 'STRING'},
        {name: 'amount_type', type: 'STRING'},
        {name: 'amount_description', type: 'STRING'},
        {name: 'concatenated_fields', type: 'STRING'},
        {name: 'remarks', type: 'STRING'},
        {name: 'account', type: 'STRING'},
        {name: 'item', type: 'STRING'},
        {name: 'tax_distiction', type: 'STRING'},

      ]
    }
  };
  try {
    BigQuery.Tables.remove(projectId, datasetId,tableId);
    Logger.log('Table removed: %s', table.id);
  } catch (err) {
    Logger.log('Table did not exist');
  }
  try {
    table = BigQuery.Tables.insert(table, projectId, datasetId);
    Logger.log('Table created: %s', table.id);
  } catch (err) {
    Logger.log('unable to create table');
  }
  // Load The sheet named Nomenclature and convert to the correct format for upload.
  var file = SpreadsheetApp.getActiveSpreadsheet();
  var currentSheet = file.getSheetByName(name);
  var lastRow = currentSheet.getLastRow();
  var lastC = currentSheet.getLastColumn();
  Logger.log(lastC);
  var rows = currentSheet.getRange(1,1,lastRow,lastC).getValues();
  var rowsCSV = rows.join("\n");
  // Logger.log(rowsCSV); // uncomment to see the logs that are being loaded
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
        skipLeadingRows: 1,
        writeDisposition:'WRITE_TRUNCATE'
      },
      useQueryCache:'False'
    }
  };
  try {
    BigQuery.Jobs.insert(job, projectId, data);
    Logger.log('Load job started. Check on the status of it here: ' +
      'https://bigquery.cloud.google.com/jobs/%s', projectId);
  } catch (err) {
    Logger.log('unable to insert job');
  }

  const request = {
    // TODO (developer) - Replace query with yours
    query:`
      ------ Apps Script Query
      ------ UPDATED 03-06
      With full_report as (
      SELECT
        CASE
          WHEN sum(suma) < 0 THEN '支出'
          WHEN sum(suma) >= 0 THEN '収入'
        END AS balance,
        CAST(settlement AS STRING) as settlement_id,
        CAST(posted_date AS DATE) as purchase_or_posted_date,
        purchase_or_posted_date as payment_date,
        'Amazon Seller' as supplier,
        Case
         when account is null then 'MISSING NOMENCLATURE'
         ELSE account
        end as  account,
        tax_distiction,
        abs(sum(suma)) as total_sum,
        '内税' as tax_calc_distinction,
        CASE WHEN remarks in ('Current Reserve Amount','Previous Reserve Amount') Then 0
        Else ROUND(abs(sum(suma)-(sum(suma)/1.1)))
        END AS tax_amount,
        remarks,
        item,
        null as department,
        null as memo,
        CURRENT_DATE() AS DATE_OF_QUERY

      FROM (
            SELECT distinct
            settlement_id as settlement,
            vm.transaction_type,
            vm.posted_date,

            CASE
              WHEN OC.purchase_date IS NULL THEN CAST(FORMAT_DATE("%Y-%m-01",vm.posted_date) AS DATE)
              ELSE CAST(FORMAT_DATE("%Y-%m-01",OC.purchase_date) AS DATE)
            END AS purchase_or_posted_date,

            vm.amount_type,
            vm.amount_description,
            sum(amount) as suma,
            Case
              When transaction_type in ('CouponRedemptionFee','Imaging Services') THEN concat(transaction_type,amount_type)
              ELSE concat(vm.transaction_type,vm.amount_type,vm.amount_description)
            End as concat_fields


            FROM `+ Amazon_datasetId + '.'+ transaction_view_tableId +` as vm
            LEFT Join
              (SELECT DISTINCT -- DISTINCT clause for picking only one order_id/purchase_date pair otherwise there are duplicates that change the totals
                -- amazon_order_id
                CASE
                  WHEN merchant_order_id is not null THEN merchant_order_id
                  ELSE amazon_order_id
                END AS amazon_order_id,
                Datetime_add(purchase_date,interval 9 HOUR) as purchase_date
              From
                `+ Amazon_datasetId + '.'+ order_central_tableId +`
              Where amazon_order_id is not null
              ) AS oc
            on vm.order_id = oc.amazon_order_id

            group by
            settlement_id,
            transaction_type,
            posted_date,
            purchase_or_posted_date,
            amount_type,
            amount_description
          ) AS sub_1
          left join `+ datasetId + '.' + tableId +`
          on sub_1.concat_fields = concatenated_fields



      where purchase_or_posted_date >= '2019-01-01'


          Group by
            settlement_id,
            purchase_or_posted_date,
            payment_date,
            supplier,
            account,
            tax_distiction,
            tax_calc_distinction,
            remarks,
            item,
            sub_1.amount_type,
            department,
            memo


          Order by settlement_id desc
          )
          SELECT
          balance,
          settlement_id,
          purchase_or_posted_date,
          CAST(deposit_date AS DATE) as payment_date,
          supplier,
          account,
          tax_distiction,
          total_sum,
          tax_calc_distinction,

          CASE
            WHEN tax_distiction = '課税売上8%（軽）' THEN  ROUND(abs(total_sum*0.08))
            WHEN tax_distiction IN ('課対仕入10%','課税売上10%') THEN  ROUND(abs(total_sum*0.1))
            ELSE 0
          END AS tax_amount,

          remarks,
          item,
          department,
          memo,
          DATE_OF_QUERY

          FROM full_report left join (SELECT DISTINCT settlement_id as sett,deposit_date FROM `+ Amazon_datasetId + '.'+ transaction_view_tableId +` where
           deposit_date is not null) as dat on CAST(dat.sett AS STRING) = full_report.settlement_id
          ---- WHERE CAST(settlement_id AS INTEGER) >= 11309997823
          ORDER BY settlement_id desc



    `
    ,
    useLegacySql: false
  };
  Logger.log('Waiting 3 seconds to bigquery cache the data');
  Utilities.sleep(5000);
  Logger.log('Starting query job');
  let queryResults = BigQuery.Jobs.query(request, projectId);
  const jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  let sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results.

  let rows_2 = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows_2 = rows_2.concat(queryResults.rows);
  }

  if (!rows_2) {
    Logger.log('No rows returned.');
    return;
  }


  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();

  var newSheet = spreadsheet.insertSheet();
  // var ui = SpreadsheetApp.getUi();
  // var result = ui.prompt("Please enter the new sheet name");
  //Get the button that the user pressed.
  // var button = result.getSelectedButton();
  // if (button === ui.Button.OK) {
  //     Logger.log("The user clicked the [OK] button.");
  //     newSheet.setName(result.getResponseText());
  //     Logger.log(result.getResponseText());
  //   } else if (button === ui.Button.CLOSE) {
  //     Logger.log("The user clicked the [X] button and closed the prompt dialog.");
  //   }

  // const sheet = spreadsheet.getActiveSheet();
  const sheet = newSheet;

  // Append the headers.
  const headers = queryResults.schema.fields.map(function(field) {
    return field.name;
  });
  // var HEADER = ["収支区分","管理番号","発生日","勘定科目","税計算区分","金額","備考","品目","amount_type","DATE_OF_QUERY"]  // Change according to the headers wanted for the results
  var HEADER= ['収支区分','管理番号','発生日','支払期日','取引先','勘定科目','税区分','金額','税計算区分','税額','備考','品目','部門','メモタグ（複数指定可、カンマ区切り）','DATE_OF_QUERY'];
  sheet.appendRow(HEADER);

  // sheet.appendRow(headers);

  // Append the results.
  var data = new Array(rows_2.length);
  for (let i = 0; i < rows_2.length; i++) {
    const cols = rows_2[i].f;
    data[i] = new Array(cols.length);
    for (let j = 0; j < cols.length; j++) {
      data[i][j] = cols[j].v;
    }
  }
  sheet.getRange(2, 1, rows_2.length, headers.length).setValues(data);


}
