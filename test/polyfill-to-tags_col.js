(async () => {
    const {sqlPromiseSafe, sqlPromiseSimple} = require("./../utils/sqlClient");

    const baseSelect = `SELECT eid FROM kanmi_records WHERE tags IS NOT NULL`
    const selectTags = `SELECT eid, GROUP_CONCAT(type,'/',ROUND(rating,4),'/',name SEPARATOR '; ') AS tags FROM sequenzia_index_matches, sequenzia_index_tags WHERE (sequenzia_index_tags.id = sequenzia_index_matches.tag) GROUP BY eid`
    const query = `SELECT base.eid, tags.tags FROM (${baseSelect}) base INNER JOIN (${selectTags}) tags ON (tags.eid = base.eid)`

    const items = (await sqlPromiseSafe(query)).rows.filter(e => !!e.eid && !!e.tags);
    let i = 0;
    const t = items.length;
    let msgRequests = items.reduce((promiseChain, e, i, a) => {
        return promiseChain.then(() => new Promise(async(completed) => {
            i++
            await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ e.tags, e.eid ]);
            console.log(`${i} rows updated out of ${t} rows`);
            completed();
        }))
    }, Promise.resolve());
    msgRequests.then(async () => {
        console.log('Successfully updated')
    })
})()

