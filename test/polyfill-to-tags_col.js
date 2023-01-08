(async () => {
    const {sqlPromiseSafe, sqlPromiseSimple} = require("./../utils/sqlClient");

    const baseSelect = `SELECT eid, tags FROM kanmi_records`
    const selectTags = `SELECT eid, GROUP_CONCAT(type,'/',ROUND(rating,4),'/',name SEPARATOR '; ') AS tags FROM sequenzia_index_matches, sequenzia_index_tags WHERE (sequenzia_index_tags.id = sequenzia_index_matches.tag) GROUP BY eid`
    const query = `SELECT base.eid, tags.tags, base.tags AS org FROM (${baseSelect}) base LEFT JOIN (${selectTags}) tags ON (tags.eid = base.eid)`

    const items = (await sqlPromiseSafe(query)).rows.filter(e => !!e.eid && !!e.tags && (!e.org || e.tags.trim() !== e.org.trim()));
    let i = 0;
    const t = items.length;
    let msgRequests = items.reduce((promiseChain, e, i, a) => {
        return promiseChain.then(() => new Promise(async(completed) => {
            i++
            await sqlPromiseSafe(`UPDATE kanmi_records SET tags = ? WHERE eid = ?`, [ e.tags, e.eid ]);
            completed();
            console.log(`${i}/${t}`);
        }))
    }, Promise.resolve());
    msgRequests.then(async () => {
        console.log('Successfully updated')
    })
})()

