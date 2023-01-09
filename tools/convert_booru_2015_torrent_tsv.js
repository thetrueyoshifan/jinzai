(async () => {
    const {StringStream} = require("scramjet");
    const fs_extra = require('fs-extra');
    const fs = require('fs');
    const crypto = require('crypto');
    const sqlite3 = require('sqlite3').verbose();

    let dataset_results = {}
    let tags_table = {}
    let tags_list = {}
    let booru_id = {}
    let booru_index = 0;
    let category = [
        "General",
        "Artist",
        "Copyright",
        "Character",
        "Other"
    ]
    await (StringStream.from(fs.createReadStream('./../../BOORU_2015/BC_2015_TAGS.tsv'))
        // read the file
        .CSVParse({delimiter: "\t", header: true})
        // parse as csv
        .map((entry) => {
            if (!booru_id[entry.BOORU]) {
                booru_id[entry.BOORU] = booru_index;
                booru_index++;
            }
            if (!tags_list[booru_id[entry.BOORU]])
                tags_list[booru_id[entry.BOORU]] = {}
            if (!tags_list[booru_id[entry.BOORU]][entry.TAG])
                tags_list[booru_id[entry.BOORU]][entry.TAG] = {category: entry.TAG_CAT, id: entry.TAG_ID, dtag: entry.DANB_TAG}

            if (!tags_table[booru_id[entry.BOORU]])
                tags_table[booru_id[entry.BOORU]] = [];
            if (!tags_table[booru_id[entry.BOORU]][parseInt(entry.FID)])
                tags_table[booru_id[entry.BOORU]][parseInt(entry.FID)] = [];
            if (entry.TAG_CAT !== 4) {
                tags_table[booru_id[entry.BOORU]][parseInt(entry.FID)].push(entry.TAG.trim())
            }
        })
        .run())

    console.log('Complete Parseing Tags!')
    //console.log(Object.keys(tags_list))
    //console.log(Object.keys(tags_list).map((entry) => Object.keys(tags_list[entry])))

    await (StringStream.from(fs.createReadStream('./../../BOORU_2015/BC_2015.tsv'))
        // read the file
        .CSVParse({delimiter: "\t", header: true})
        // parse as csv
        .map((entry) => {
            if (!dataset_results[entry.BOORU])
                dataset_results[entry.BOORU] = {};
            const totalTagCount = parseInt(entry.TAGS_G) + parseInt(entry.TAGS_C) + parseInt(entry.TAGS_P) + parseInt(entry.TAGS_A)
            if (totalTagCount > 0) {
                const fake_md5 = crypto.randomBytes(32).toString("hex");
                dataset_results[entry.BOORU][parseInt(entry.FID)] = {
                    md5: fake_md5,
                    name: entry.FNAME,
                    path: entry.RPATH,
                    rank: entry.RNK,
                    general_count: entry.TAGS_G
                }
            } else {
                console.error(`No tags for ${entry.FNAME}, will be omitted!`)
            }
        })
        .run())
    console.log('Complete Parseing Dataset!')

    //const db = new sqlite3.Database(':memory:');
    const db = new sqlite3.Database('./../../BOORU_2015/danbooru.sqlite');
    db.serialize(() => {
        db.run(`DROP TABLE IF EXISTS posts`);
        db.run("CREATE TABLE posts (id INTEGER, md5 TEXT, file_ext TEXT, tag_string TEXT, tag_count_general INTEGER)");

        const stmt = db.prepare("INSERT INTO posts VALUES (?,?,?,?,?)");
        const booru_list = Object.keys(dataset_results);
        console.log('Complete Createing Database!')

        for (let booru of booru_list) {
            const booru_posts = dataset_results[booru];
            const booru_tags = tags_table[booru_id[booru]];

            for (let fileid of Object.keys(booru_posts)) {
                const file = booru_posts[fileid];
                try {
                    stmt.run(parseInt(fileid), file.md5, file.name.split(".").pop(), booru_tags[parseInt(fileid)].join(' '), file.general_count);
                    if (!fs.existsSync(`./../../BOORU_2015/${file.md5.substring(0,2)}/`))
                        fs.mkdirSync(`./../../BOORU_2015/${file.md5.substring(0,2)}/`)
                    fs_extra.moveSync(`./../../BOORU_2015/${file.path.split('\\')[0]}/${file.name}`, `./../../BOORU_2015/${file.md5.substring(0,2)}/${file.md5}.${file.name.split(".").pop()}`);
                } catch (err) {
                    console.error(fileid + ' from ' + booru + ' no tags!')
                }
            }

        }
        console.log('Complete Writing Database!')
        stmt.finalize();

        db.each("SELECT * FROM posts LIMIT 100", (err, row) => {
            console.log(row);
        });
    });
    db.close();
    console.log('Complete!')
})()

