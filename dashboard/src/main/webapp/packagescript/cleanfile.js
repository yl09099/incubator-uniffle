const fs = require('fs')
const p = require('path')

const nodeModulesPath = p.join(__dirname, '../node_modules')
const lockJsonPath = p.join(__dirname, '../package-lock.json')

if (fs.existsSync(nodeModulesPath)) {
    const fileUtil = require('./fileutils')

    //fileUtil.deleteFolderByRimraf(nodeModulesPath)
    fileUtil.deleteFolder(nodeModulesPath)
    console.log('Deleted node_modules successfully!')

    fileUtil.deleteFile(lockJsonPath)
    console.log('Delete package-lock.json successfully!')
}
