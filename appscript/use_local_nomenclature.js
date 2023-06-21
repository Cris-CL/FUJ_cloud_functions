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
      --- NEW AMAZON Q
      --- sheets version
      with sub as (
      with tor as(
      with tv as (
        select
      CAST(settlement_id AS INT64) as settlement_id,
      DATETIME_ADD(posted_date_time, interval 9 HOUR) as posted_date_time,
      transaction_type,
      amount_type,
      amount_description,
      sum(amount) as total_amount,
      order_id,
      sku,
      Case
        When transaction_type in ('CouponRedemptionFee','Imaging Services') THEN concat(transaction_type,amount_type)
        ELSE concat(transaction_type,amount_type,amount_description)
      End as concat_fields,
      sum(quantity_purchased) as total_purchased,
      from Amazon.transaction_view_master
      group by

      settlement_id,
      posted_date_time,
      transaction_type,
      order_id,
      amount_type,
      amount_description,
      sku
      )
      SELECT
      tv.* except(posted_date_time),
      tv.posted_date_time,
      oc.purchase_date,

      CASE ------REFUNDS APPEAR AT THE SETTLEMENT DATE NOW
        WHEN transaction_type = 'Refund' THEN tv.posted_date_time
        WHEN oc.purchase_date is null THEN tv.posted_date_time
        ELSE oc.purchase_date
      END AS main_date,
      CASE
        WHEN transaction_type = 'Refund' THEN 'POSTED_DATE_REFUND'
        WHEN oc.purchase_date is null THEN 'POSTED_DATE'
        ELSE 'ORDER_DATE'
      END AS DATE_TYPE

      from tv
      LEFT JOIN (SELECT DISTINCT
      amazon_order_id,
      DATETIME_ADD(purchase_date,INTERVAL 9 HOUR) as purchase_date
      FROM Amazon.order_central_master) as oc
      ON oc.amazon_order_id = tv.order_id

      )

      SELECT
        CASE
          WHEN sum(total_amount) < 0 THEN '支出'
          WHEN sum(total_amount) >= 0 THEN '収入'
        END AS balance,
        CAST(settlement_id AS STRING) as settlement_id,
        CAST(main_date AS DATE) as main_date,
        -- purchase_or_posted_date as payment_date,
        'Amazon Seller' as supplier,
        Case
          when account is null then 'MISSING NOMENCLATURE'
          ELSE account
        end as  account,
        tax_distiction,
        abs(sum(total_amount)) as total_sum,
        '内税' as tax_calc_distinction,
        remarks,
        item,
        null as department,
        null as memo,
        DATE_TYPE,
        sku,
        total_purchased,
        sum(total_amount) total_real,
        order_id,
        CAST(posted_date_time AS DATE) AS posted,
        main_date as full_date,
        CURRENT_DATE() AS DATE_OF_QUERY

      FROM tor
      LEFT JOIN temp_nomenclature.nomenclature_temp
          on tor.concat_fields = concatenated_fields
      WHERE main_date is not null
      GROUP BY
      settlement_id,
      main_date,
      full_date,
      account,
      tax_distiction,
      tax_calc_distinction,
      remarks,
      item,
      department,
      memo,
      DATE_TYPE,
      sku,
      order_id,
      posted_date_time,
      total_purchased
      )

      SELECT
        CASE
          WHEN sum(total_real)  < 0 THEN '支出'
          WHEN sum(total_real)  >= 0 THEN '収入'
        END AS balance,
      settlement_id,
      cast(main_date as DATE) as main_date,
      CAST(DATETIME_ADD(dat.deposit_date, INTERVAL 9 HOUR) AS DATE) as deposit_date,
      supplier,
      account,
      tax_distiction,
      abs(sum(total_real)) as total,
      tax_calc_distinction,
      CASE
        when tax_distiction like '%8%' THEN ABS(ROUND(sum(total_real) *0.08))
        when tax_distiction like '%10%' THEN ABS(ROUND(sum(total_real) *0.1))
        ELSE 0
      END AS tax,
      remarks,
      item,
      null as department,
      null as memo

      from sub left join
      (SELECT DISTINCT settlement_id as sett,deposit_date FROM Amazon.transaction_view_master where
            deposit_date is not null) as dat on sub.settlement_id = CAST(CAST(dat.sett AS INT64) AS STRING)
      group by
      settlement_id,
      remarks,
      main_date,
      deposit_date,
      supplier,
      account,
      item,
      tax_distiction,
      tax_calc_distinction

      having sum(total_real) <> 0

      -- 475032
      order by settlement_id desc

    `,
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
  var HEADER= ['収支区分','管理番号','発生日','支払期日','取引先','勘定科目','税区分','金額','税計算区分','税額','備考','品目','部門','メモタグ（複数指定可、カンマ区切り）'];
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
