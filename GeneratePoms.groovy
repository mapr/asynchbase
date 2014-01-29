import groovy.util.NodeBuilder
import groovy.util.XmlNodePrinter
import groovy.util.XmlParser
/**
 * @author Patrick Wong
 */
class GenerateUserFacingPoms {
    static void main(String[] args) {
        if (args.length != 2) {
            System.err << "Need exactly two args: the full asynchbase version, and the full mapr release version"
            System.exit(1)
        }

        String asynchbaseVersion = args[0]
        String maprReleaseVersion = args[1]
        println "Full HBase version was read as: " + asynchbaseVersion
        println "Full mapr-hbase version was read as: " + maprReleaseVersion

        File originalPomFile = new File("pom.xml")
        XmlParser xmlIn = new XmlParser()
        xmlIn.setTrimWhitespace(true)
        Node pomTree = xmlIn.parse(originalPomFile)

        Node newPomTree = pomTree.clone()
        newPomTree.version[0].setValue(asynchbaseVersion)
        newPomTree.properties[0]."mapr.release.version"[0].setValue(maprReleaseVersion)

        /*
        newPomTree.dependencies[0].removeAll { Node dependency ->
            dependency.groupId[0].textValue().contains("com.mapr")
        }
        */

        StringWriter writer = new StringWriter()
        XmlNodePrinter xmlOut = new XmlNodePrinter(new PrintWriter(writer))
        xmlOut.setPreserveWhitespace(true)
        xmlOut.setExpandEmptyElements(false)
        xmlOut.print(newPomTree)
        File outputPomFile = new File("pom-userfacing-" + maprReleaseVersion + "-generated.xml")
        outputPomFile.write(writer.toString())
    }
}