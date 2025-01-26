{{R3M}}
{{$request = request()}}
{{$options = options()}}
{{if(is.empty($options.url))}}
{{terminal.error('You need to provide the option (url) to provide the source file of the import.' + PHP_EOL)}}
{{/if}}
Package: {{$request.package}}

Module: {{$request.module|uppercase.first}}

Submodule: {{$request.submodule|uppercase.first}}

{{$class = data.extract('options.class')}}
{{if(is.empty($class))}}
{{terminal.error('You need to provide the option class.' + PHP_EOL)}}
Available classes:
{{$read = dir.read(config('project.dir.node') + 'Object/')}}
{{if(is.array($read))}}
{{$read = data.sort($read, ['name' => 'ASC'])}}
{{for.each($read as $file)}}
- {{file.basename($file.name, config('extension.json'))}}

{{/for.each}}
{{/if}}
{{else}}
{{$response = Raxon.Node:Data:import(
$class,
Raxon.Node:Role:role.system(),
$options
)}}
{{$response|json.encode:'JSON_PRETTY_PRINT'}}

{{/if}}