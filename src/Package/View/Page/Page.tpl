{{R3M}}
{{$request = request()}}
{{$options = options()}}
{{$class = data.extract('options.class')}}
{{if(is.empty($class))}}
You need to provide the option class.
Available classes:
{{$read = dir.read(config('project.dir.node') + 'Object/')}}
{{if(is.array($read))}}
{{$read = data.sort($read, ['name' => 'ASC'])}}
{{for.each($read as $file)}}
- {{file.basename($file.name, config('extension.json'))}}

{{/for.each}}
{{/if}}
{{else}}
{{if(is.empty($options.sort))}}
{{$options.sort = [
'uuid' => 'ASC'
]}}
{{/if}}
{{if(is.empty($options.uuid))}}
You can use list to get the uuid.
{{else}}
{{$response = Raxon.Node:Data:page(
$class,
Raxon.Node:Role:role_system(),
$options
)}}
{{$response|json.encode:'JSON_PRETTY_PRINT'}}

{{/if}}
{{/if}}