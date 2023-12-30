    //
    //  JVFileManager.swift.swift
    //
    //
    //  Created by Jan Verrept on 26/12/2019.
    //
import Foundation
import OSLog


public extension FileManager{
    
    func checkForDirectory(_ directoryURL:URL, createIfNeeded:Bool = false){
        let logger = Logger(subsystem: "be.oneclick.JVSwift", category: "JVFileManager")
        
        logger.info("Checking for folder at \(directoryURL.path, privacy: .public)")
        var isFolder:ObjCBool = false
        let supportFolderExists = FileManager.default.fileExists(atPath: directoryURL.path, isDirectory: &isFolder) && isFolder.boolValue
        if !supportFolderExists && createIfNeeded{
            do {
                try FileManager.default.createDirectory(atPath: directoryURL.path, withIntermediateDirectories: true, attributes: nil)
                logger.info("✅\tCreated folder @\(directoryURL.path, privacy: .public)")
            } catch {
                logger.error(
                    """
                    Could not create folder @\(directoryURL.path, privacy: .public):
                    \(error.localizedDescription, privacy: .public)
                    """
                )
            }
        }
    }
    
    func rename(_ originalURL:URL, to newURL:URL){
        let logger = Logger(subsystem: "be.oneclick.JVSwift", category: "JVFileManager")
        
        let originalName = originalURL.lastPathComponent
        let newName = newURL.lastPathComponent
        do {
            if fileExists(atPath: newURL.path){
                try removeItem(at:newURL)
            }
            try moveItem(at: originalURL, to: newURL)
        } catch {
            logger.error(
                """
                Could not rename file from \(originalName, privacy: .public) to \(newName, privacy: .public):
                \(error.localizedDescription, privacy: .public)
                """
            )
        }
        
    }
    
    func createAlias(from originalURL:URL, at aliasURL:URL){
        let logger = Logger(subsystem: "be.oneclick.JVSwift", category: "JVFileManager")
        
        do{
            let bookmarkData = try originalURL.bookmarkData(options: URL.BookmarkCreationOptions.suitableForBookmarkFile, includingResourceValuesForKeys: nil, relativeTo: nil)
            try URL.writeBookmarkData(bookmarkData, to: aliasURL)
        } catch {
            logger.error(
                """
                Could not creat alias of \(originalURL, privacy: .public) @ \(aliasURL, privacy: .public):
                \(error.localizedDescription, privacy: .public)
                """
            )
        }
        
    }
    
    func createShortcut(from originalURL:URL, at aliasURL:URL){
        createAlias(from: originalURL, at: aliasURL)
    }
    
    
    
}

