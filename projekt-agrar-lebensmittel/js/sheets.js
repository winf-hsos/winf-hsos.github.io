async function readSheetData(workbookId, sheetNumber) {

    let values;
    let json;

    try {
        values = await fetch('https://spreadsheets.google.com/feeds/list/' + workbookId + '/' + sheetNumber + '/public/values?alt=json');
        json = await values.json();
    }
    catch (error) {
        if (error.name === 'FetchError') {
            console.error("No data returned. Maybe sheet not published to web, wrong workbook ID, or sheet " + sheetNumber + " does not exist in sheet?");
        }

        return { "error": "No data returned. Maybe sheet not published to web, wrong workbook ID, or sheet " + sheetNumber + " does not exist in sheet?" };
    }


    let rows = json.feed.entry;

    let data = {};
    data.title = json.feed.title['$t']
    let dataRows = [];

    for (let i = 0; i < rows.length; i++) {
        let row = rows[i];
        let rowObj = {}
        for (column in row) {

            if (column.startsWith('gsx$')) {
                let columnName = column.split("$")[1];
                rowObj[columnName] = row[column]["$t"];
            }
        }
        dataRows.push(rowObj);
    }

    data.rows = dataRows;

    return data;
}