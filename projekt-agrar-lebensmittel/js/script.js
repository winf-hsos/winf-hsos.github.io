
Promise.all([
    readSheetData("1Ve7ZlI5FrTtEBEYfixe98JOuwKWKc1dXd_vgM6d2BB8", 2),
    readSheetData("1Ve7ZlI5FrTtEBEYfixe98JOuwKWKc1dXd_vgM6d2BB8", 1)]).then(start);

var projects, participants;

function start(data) {

    projects = data[0];
    participants = data[1];

    // Merge projects and participants
    let merged = mergeProjectsAndParticipantsData();

    let projectTable = document.querySelector("#projectTableRows");

    for (let i = 0; i < merged.length; i++) {
        if (merged[i].participants.length > 0) {
            let projectRowElement = createProjectTableRowItem(i + 1, merged[i]);
            projectTable.appendChild(projectRowElement);
        }
    }
}


function createProjectTableRowItem(nr, project) {

    let tableRowElement = document.createElement('tr');

    let columnElement = document.createElement('td');
    columnElement.textContent = nr;
    tableRowElement.appendChild(columnElement);

    // titel
    columnElement = document.createElement('td');
    columnElement.textContent = project.titel;
    tableRowElement.appendChild(columnElement);

    // betreuer/in
    columnElement = document.createElement('td');
    columnElement.textContent = project.nachnamebetreuer;
    tableRowElement.appendChild(columnElement);

    // participants
    columnElement = document.createElement('td');
    columnElement.innerHTML = getParticipantsString(project.projektid);
    tableRowElement.appendChild(columnElement);


    return tableRowElement;

}


function getParticipantsString(projectId) {

    let result = "";
    for (let i = 0; i < participants.rows.length; i++) {

        if (participants.rows[i].projektid === projectId) {

            result += participants.rows[i].email + "<br>";
        }
    }

    return result;

}


function mergeProjectsAndParticipantsData() {

    var result = [];
    for (let i = 0; i < projects.rows.length; i++) {

        let project = projects.rows[i];
        project.participants = [];

        for (let j = 0; j < participants.rows.length; j++) {


            if (participants.rows[j].projektid === project.projektid) {
                project.participants.push(participants.rows[j]);
            }
        }

        result.push(project);
    }

    return result;

}