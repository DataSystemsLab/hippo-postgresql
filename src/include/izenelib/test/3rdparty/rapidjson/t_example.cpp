#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/filestream.h"

using namespace rapidjson;

void t_condense();

int main(int argc, char* argv[])
{
    //t_condense();
}


// JSON condenser exmaple
// This example parses JSON text from stdin with validation,
// and re-output the JSON content to stdout without whitespace.
void t_condense()
{
    // Prepare JSON reader and input stream.
    Reader reader;
    FileStream is(stdin);

    // Prepare JSON writer and output stream.
    FileStream os(stdout);
    Writer<FileStream> writer(os);

    // JSON reader parse from the input stream and let writer generate the output.
    if (!reader.Parse<0>(is, writer)) {
        fprintf(stderr, "\nError(%u): %s\n", (unsigned)reader.GetErrorOffset(), reader.GetParseError());
    }
}
