{{R3M}}
{{$request = request()}}
{{$options = options()}}
{{$class = data.extract('options.class')}}
{{$response = Raxon.Node:Data:put(
$class,
Raxon.Node:Role:role_system(),
$options
)}}
{{$response|json.encode:'JSON_PRETTY_PRINT'}}

