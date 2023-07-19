const fs = require('fs')

/**
 * Delete folder
 * @param path Path of the folder to be deleted
 */
function deleteFolder (path) {
    let files = [];
    if (fs.existsSync(path)) {
        if (fs.statSync(path).isDirectory()) {
            files = fs.readdirSync(path)
            files.forEach((file) => {
                const curPath = path + '/' + file;
                if (fs.statSync(curPath).isDirectory()) {
                    deleteFolder(curPath)
                } else {
                    fs.unlinkSync(curPath)
                }
            })
            fs.rmdirSync(path)
        } else {
            fs.unlinkSync(path)
        }
    }
}

/**
 * Delete file
 * @param path Path of the file to be deleted
 */
function deleteFile (path) {
    if (fs.existsSync(path)) {
        if (fs.statSync(path).isDirectory()) {
            deleteFolder(path)
        } else {
            fs.unlinkSync(path)
        }
    }
}

/**
 * Copy the folder to the specified directory
 * @param from Source directory
 * @param to Target directory
 */
function copyFolder (from, to) {
    let files = []
    // Whether the file exists If it does not exist, it is created
    if (fs.existsSync(to)) {
        files = fs.readdirSync(from)
        files.forEach((file) => {
            const targetPath = from + '/' + file;
            const toPath = to + '/' + file;

            // Copy folder
            if (fs.statSync(targetPath).isDirectory()) {
                copyFolder(targetPath, toPath)
            } else {
                // Copy file
                fs.copyFileSync(targetPath, toPath)
            }
        })
    } else {
        fs.mkdirSync(to)
        copyFolder(from, to)
    }
}

module.exports = {
    deleteFolder,
    deleteFile,
    copyFolder
}
