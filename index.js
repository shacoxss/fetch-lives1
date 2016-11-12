"use strict"

const await_for = require('./await_for')
const lives = require('./lives')
const f = require('./collect')
const mysql = require('mysql2/promise')

const DB = {
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'fs'
}
const table = 'anchors'
const cols = ['id', 'live_id', 'room_id', 'category_id', 'name', 'room_name', 'url', 'avatar', 'cover', 'online']
const off_line = 'status = 0'

const begin = tag => 
    _ => {
        console.time(tag)
        return _
    }
const end = tag => 
    _ => {
        console.timeEnd(tag)
        return _
    }
const end_begin = (tag, tag_begin) => 
_ => {
    console.timeEnd(tag)
    console.time(tag_begin)
    return _
}

const requests = f.collect(lives)
    .map(f.call)
    .map(f.collect(10).gen_fill_closure(await_for))
    .collapse()

console.time('fetch')
Promise.all(requests)
    .then(end_begin('fetch', 'filter'))
    .then(f.collapse)
    .then(f.unique('id'))
    .then(end_begin('filter', 'query'))
    .then(data => {
        let connect = null
        const gen_query = sql => () => connect.query(sql)

        const ids = `("${f.pluck(data, 'id').join('","')}")`

        const needle_sql = gen_query(`SELECT id FROM ${table} WHERE id IN ${ids} AND ${off_line}`)

        const delete_sql = gen_query(`DELETE FROM ${table} WHERE id IN ${ids}`)

        const update_sql = gen_query(`UPDATE ${table} SET ${off_line}`)

        const insert_sql = gen_query(`INSERT INTO ${table} (${cols.join(',')},status) VALUES ${
            data.map(row => `(${cols.map(col => `'${row[col]}'`).join(',')},1)`).join(',')
        }`)

        mysql.createConnection(DB)
            .then(c => connect = c)
            .then(needle_sql)
            .then(needles => null /*循环发送通知*/)
            .then(delete_sql)
            .then(update_sql)
            .then(insert_sql)
            .then(_=> connect.end())
            .then(end('query'))
            .catch(sql_error => {
                connect.end()
                console.error(sql_error)
            })
    })
    .catch(console.error)