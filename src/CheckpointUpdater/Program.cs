// See https://aka.ms/new-console-template for more information

using CorshamScience.MessageDispatch.EventStore;

var filePath = args[0];

if (!File.Exists(filePath))
{
    Console.WriteLine($"Could not find checkpoint file at \"{filePath}\"");
    return 1;
}

var chk = new WriteThroughFileCheckpoint(filePath);

Console.WriteLine($"Checkpoint has existing value:{chk.Read()}");

Console.WriteLine("Change this value? [Y]es/[N]o/[I]ncrement");

var input = Console.ReadLine();
var trigger = input!.ToUpper()[0];
switch (trigger)
{
    case 'Y':
        Console.WriteLine("Enter a new value: ");
        var newValue = Console.ReadLine();
        var newValueLong = long.Parse(newValue!);
        chk.Write(newValueLong);
        break;
    case 'I':
        chk.Write(chk.Read() + 1);
        break;
    case 'N':
        return 0;
    default:
        Console.WriteLine($"Unexpected Input beginning with {trigger}");
        return 3;
}

Console.WriteLine($"Checkpoint updated - now has value: {chk.Read()}");
return 0;