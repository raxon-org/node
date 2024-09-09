{{R3M}}
{{$request = request()}}
{{$options = options()}}
{{$from = data.extract('options.from')}}
{{if(is.empty($from))}}
You need to provide the option "from".
Available classes:
{{$read = dir.read(config('project.dir.node') + 'Object/')}}
{{if(is.array($read))}}
{{$read = data.sort($read, ['name' => 'ASC'])}}
{{for.each($read as $file)}}
- {{file.basename($file.name, config('extension.json'))}}

{{/for.each}}
{{/if}}
{{/if}}
{{$to = data.extract('options.to')}}
{{if(is.empty($to))}}
You need to provide the option "to".
Available classes:
{{$read = dir.read(config('project.dir.node') + 'Object/')}}
{{if(is.array($read))}}
{{$read = data.sort($read, ['name' => 'ASC'])}}
{{for.each($read as $file)}}
- {{file.basename($file.name, config('extension.json'))}}

{{/for.each}}
{{/if}}
{{/if}}
{{if(
!is.empty($from) &&
!is.empty($to)
)}}
{{$response = Raxon.Org.Node:Data:rename(
$from,
$to,
Raxon.Org.Node:Role:role_system(),
$options
)}}
{{$response|json.encode:'JSON_PRETTY_PRINT'}}

{{/if}}