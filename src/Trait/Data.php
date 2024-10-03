<?php
namespace Raxon\Node\Trait;

use Exception;
use Raxon\App;
use Raxon\Config;
use Raxon\Exception\ObjectException;
use Raxon\Module\Cli;
use Raxon\Module\Controller;
use Raxon\Module\Core;
use Raxon\Module\Data as Storage;
use Raxon\Module\Dir;
use Raxon\Module\File;
use Raxon\Node\Service\Security;

trait Data {

    use Compact;
    use Compress;
    use Expand;
    use Expose;
    use Extract;
    use Filter;
    use Relation;
    use Stats;
    use Transaction;
    use Tree;
    use Validate;
    use View;
    use Where;


    use Data\Count;
    use Data\Create;
    use Data\Delete;
    use Data\Import;
    use Data\Index;
    use Data\Lock;
    use Data\NodeList;
    use Data\Page;
    use Data\Patch;
    use Data\Property;
    use Data\Put;
    use Data\Read;
    use Data\Record;
    use Data\Rename;
    use Data\Single;

    /**
     * @throws Exception
     */
    public function object_create($class, $role, $node=[], $options=[]): void
    {
        $name = Controller::name($class);
        $object = $this->object();
        $object->request('node', (object) $node);
        $dir_node = $object->config('project.dir.node');
        $dir_validate = $dir_node .
            'Validate'.
            $object->config('ds')
        ;
        $dir_data = $dir_node .
            'Data'.
            $object->config('ds')
        ;
        $dir_object = $dir_node .
            'Object'.
            $object->config('ds')
        ;
        $dir_expose = $dir_node .
            'Expose'.
            $object->config('ds')
        ;
        if(!array_key_exists('function', $options)){
            $options['function'] = str_replace('_', '.', __FUNCTION__);
        }
        $options['relation'] = false;
        $force = false;
        if(
            array_key_exists('force', $options) &&
            $options['force'] === true
        ){
            $force = $options['force'];
            unset($options['force']);
        }
        if(!Security::is_granted(
            'Data',
            $role,
            $options
        )){
            return;
        }
        Core::interactive();
        $url = $dir_data .
            $name .
            $object->config('extension.json')
        ;
        $url_object = $dir_object .
            $name .
            $object->config('extension.json')
        ;
        $url_expose = $dir_expose .
            $name .
            $object->config('extension.json')
        ;
        $url_validate = $dir_validate .
            $name .
            $object->config('extension.json')
        ;
        if(
            $force === true ||
            (
                !File::exist($url_object) &&
                !File::exist($url_expose) &&
                !File::exist($url_validate)
            )
        ){
            $item = [];
            $item['Node'] = [];
            $item['Node']['#class'] = $name;
            $item['Node']['type'] = 'object';
            $item['Node']['property'] = $this->object_create_property($object, $name);
            if(
                !empty($item['Node']['property']) &&
                is_array($item['Node']['property'])
            ){
                $is_uuid = false;
                $is_class = false;
                foreach($item['Node']['property'] as $nr => $property){
                    if(
                        is_object($property) &&
                        property_exists($property, 'name') &&
                        $property->name === 'uuid'
                    ){
                        $is_uuid = true;

                    }
                    if(
                        is_object($property) &&
                        property_exists($property, 'name') &&
                        $property->name === '#class'
                    ){
                        $is_class = true;
                    }
                    if(
                        $is_uuid &&
                        $is_class
                    ){
                        break;
                    }
                }
                if($is_uuid === false){
                    $item['Node']['property'][] = (object) [
                        'name' => 'uuid',
                        'type' => 'uuid'
                    ];
                }
                if($is_class === false){
                    $item['Node']['property'][] = (object) [
                        'name' => '#class',
                        'type' => 'string'
                    ];
                }
            }
            $item['is.unique'] = $this->object_create_is_unique($object, $name);
            $item = (object) $item;
            Dir::create($dir_data, Dir::CHMOD);
            Dir::create($dir_expose, Dir::CHMOD);
            File::write($url_object, Core::object($item, Core::OBJECT_JSON));
            echo 'Written to:' . PHP_EOL;
            echo '- ' . $url_object . PHP_EOL;
            $expose = $this->object_create_expose($object, $name, $item);
            File::write($url_expose, Core::object($expose, Core::OBJECT_JSON));
            echo '- ' . $url_expose . PHP_EOL;
            $validate = $this->object_create_validate($object, [
                'class' => $name,
                'is.unique' => $item->{'is.unique'}
            ]);
            if($validate){
                File::write($url_validate, Core::object($validate, Core::OBJECT_JSON));
                echo '- ' . $url_validate . PHP_EOL;
            }
            File::permission($object, [
                'dir_object' => $dir_object,
                'dir_data' => $dir_data,
                'dir_expose' => $dir_expose,
                'dir_validate' => $dir_validate,
                'url' => $url,
                'url_object' => $url_object,
                'url_expose' => $url_expose,
                'url_validate' => $url_validate,
            ]);
        } else {
            throw new Exception('Object already exists: ' . $url . ', ' . $url_expose .', ' .  $url_object .' or ' . $url_validate . '.');
        }
    }

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function object_create_validate(App $object, $options=[]): mixed
    {
        if(!array_key_exists('class', $options)){
            return false;
        }
        $uuid = [];
        $uuid[] = (object) [
            'is.uuid' => true
        ];
        $validate = new Storage();
        $validate->set($options['class'] . '.create.validate.uuid', $uuid);
        $validate->set($options['class'] . '.put.validate.uuid', $uuid);
        $validate->set($options['class'] . '.patch.validate.uuid', $uuid);
        $is_unique = [];
        if(
            array_key_exists('is.unique', $options) &&
            !empty($options['is.unique']) &&
            is_array($options['is.unique'])
        ){
            $action = false;
            foreach ($options['is.unique'] as $nr => $string){
                $attribute = explode(',', $string);
                foreach($attribute as $attribute_nr => $value){
                    $attribute[$attribute_nr] = trim($value);
                }
                $action = mb_strtolower($attribute[0]);
                $is_unique[] = (object) [
                    'is.unique' => [
                        'class' => $options['class'],
                        'attribute' => $attribute
                    ]
                ];
            }
            if($action){
                $validate->set($options['class'] . '.create.validate.' . $action, $is_unique);
                $validate->set($options['class'] . '.put.validate.' . $action, $is_unique);
                $validate->set($options['class'] . '.patch.validate.' . $action, $is_unique);
            }
        }
        return $validate->data();
    }

    /**
     * @throws Exception
     */
    public function object_create_expose(App $object, $class, $item): mixed
    {
        $data = new Storage();
        $item = new Storage($item);
        $expose = new Storage();
        $expose->set('role', 'ROLE_SYSTEM');
        $attributes = [];
        $attributes[] = 'uuid';
        $attributes[] = '#class';
        $properties = $item->get('Node.property');
        if($properties){
            foreach($properties as $nr => $property){
                if(
                    property_exists($property, 'name') &&
                    !in_array($property->name, $attributes, true)
                ){
                    $attributes[] = $property->name;
                }
            }
        }
        $expose->set('property', $attributes);
        $objects = $this->object_create_expose_object($object, $class, $item->get('Node.property'));
        if(
            !empty($objects) &&
            is_array($objects) &&
            array_key_exists('object', $objects)
        ){
            $expose->set('object', $objects['object']);
        }
        $data->set($class . '.count.output', [ $expose->data() ]);
        $data->set($class . '.create.output', [ $expose->data() ]);
        $data->set($class . '.create_many.output', [ $expose->data() ]);
        $data->set($class . '.list.output', [ $expose->data() ]);
        $data->set($class . '.page.output', [ $expose->data() ]);
        $data->set($class . '.patch.output', [ $expose->data() ]);
        $data->set($class . '.patch_many.output', [ $expose->data() ]);
        $data->set($class . '.put.output', [ $expose->data() ]);
        $data->set($class . '.put_many.output', [ $expose->data() ]);
        $data->set($class . '.read.output', [ $expose->data() ]);
        $data->set($class . '.record.output', [ $expose->data() ]);
        return $data->data();
    }

    public function object_create_expose_object($object, $class, $properties=[]): array
    {
        $result = [];
        foreach($properties as $nr => $property){
            if(
                property_exists($property, 'name') &&
                property_exists($property, 'type') &&
                $property->type === 'object'
            ){
                $expose = [];
                $objects = [];
                if(property_exists($property, 'property')){
                    foreach($property->property as $object_property){
                        if(property_exists($object_property, 'name')){
                            $expose[] = $object_property->name;
                        }
                        if(
                            property_exists($object_property, 'type') &&
                            $object_property->type === 'object'
                        ){
                            $objects[$object_property->name] = $this->object_create_expose_object($object, $class, $object_property->property);
                        }
                    }
                }
                $multiple = false;
                if(property_exists($property, 'multiple')){
                    $multiple = $property->multiple;
                }
                if(!empty($objects)){
                    $result['object'][$property->name] = [
                        'multiple' => $multiple,
                        'output' => $expose,
                        'object' => $objects
                    ];
                } else {
                    $result['object'][$property->name] = [
                        'multiple' => $multiple,
                        'output' => $expose
                    ];
                }
            }
        }
        return $result;
    }

    /**
     * @throws ObjectException
     */
    public function object_create_is_unique(App $object, $class): array
    {
        $is_unique = [];
        echo 'Leave "is unique" empty if finished.' . PHP_EOL;
        while(true){
            echo 'Enter the property of the "is unique"' . PHP_EOL;
            $name = Cli::read('input', '(use a , to use multiple properties): ');
            if(empty($name)){
                break;
            }
            $is_unique[] = $name;
        }
        return $is_unique;
    }

    public function object_create_type_available(): void
    {
        $array = $this->object_create_type_available_array();
        foreach($array as $nr => $type){
            echo '    - ' . $type . PHP_EOL;
        }
        flush();
    }

    public function object_create_type_available_array(): array
    {
        return [
            'array',
            'array|boolean',
            'array|date',
            'array|datetime',
            'array|dynamic',
            'array|float',
            'array|integer',
            'array|microtime',
            'array|null',
            'array|object',
            'array|scalar',
            'array|string',
            'array|time',
            'array|uuid',
            'boolean',
            'date',
            'datetime',
            'dynamic',
            'float',
            'integer',
            'microtime',
            'null',
            'object',
            'scalar',
            'string',
            'time',
            'uuid',
            'relation',
        ];
    }

    /**
     * @throws ObjectException
     */
    public function object_create_property(App $object, $class, $deep=1): array
    {
        $properties = [];
        echo 'Leave "name" empty if finished.' . PHP_EOL;
        fwrite(STDOUT, 'Leave "name" empty if finished.' . PHP_EOL);
        while(true){
            $name = Cli::read('input', 'Enter the "name" of the property: ');
            if(empty($name)){
                if($deep > 1){
                    echo 'Object added...' . PHP_EOL;
                }
                break;
            }
            echo 'Available types:' . PHP_EOL;
            $this->object_create_type_available();
            $type = Cli::read('input', 'Enter the "type" of the property2: ');
            $type = Cli::read('input', 'Enter the "type" of the property: ');
            while(
            !in_array(
                $type,
                $this->object_create_type_available_array(),
                true
            )
            ){
                echo 'Available types:' . PHP_EOL;
                $this->object_create_type_available();
                $type = Cli::read('input', 'Enter the "type" of the property: ');
            }
            if($type === 'relation'){
                $is_multiple_relation = Cli::read('input', 'Are there multiple relations (y/n): ');
                if($is_multiple_relation === 'y'){
                    $is_multiple_relation = true;
                } else {
                    $is_multiple_relation = false;
                }
                if($is_multiple_relation){
                    $properties[] = (object) [
                        'name' => $name,
                        'type' => 'array',
                        'relation' => true,
                        'is_multiple' => $is_multiple_relation,
                    ];
                } else {
                    $properties[] = (object) [
                        'name' => $name,
                        'type' => 'uuid',
                        'relation' => true,
                        'is_multiple' => $is_multiple_relation,
                    ];
                }
            }
            elseif($type === 'dynamic'){
                $is_multiple_dynamic = Cli::read('input', 'Are there multiple dynamics (y/n): ');
                if($is_multiple_dynamic === 'y'){
                    $is_multiple_dynamic = true;
                } else {
                    $is_multiple_dynamic = false;
                }
                if($is_multiple_dynamic){
                    $properties[] = (object) [
                        'name' => $name,
                        'type' => 'array',
                        'dynamic' => true,
                        'is_multiple' => true,
                    ];
                } else {
                    $properties[] = (object) [
                        'name' => $name,
                        'type' => 'object',
                        'dynamic' => true,
                        'is_multiple' => false,
                    ];
                }
            }
            elseif($type === 'object'){
                $is_multiple = Cli::read('input', 'Are there multiple objects (y/n): ');
                if($is_multiple === 'y'){
                    $is_multiple = true;
                } else {
                    $is_multiple = false;
                }
                echo 'Please enter the "properties" of the object.' . PHP_EOL;
                $has_property_properties = [];
                while(true){
                    $has_property_name = Cli::read('input', 'Enter the "name" of the property: ');
                    if(empty($has_property_name)){
                        break;
                    }
                    $has_property_type = null;
                    while(
                        !in_array(
                            $has_property_type,
                            $this->object_create_type_available_array(),
                            true
                        )
                    ){
                        echo 'Available types:' . PHP_EOL;
                        $this->object_create_type_available();
                        $has_property_type = Cli::read('input', 'Enter the "type" of the property: ');
                    }
                    if($has_property_type === 'relation'){
                        $is_multiple_relation = Cli::read('input', 'Are there multiple relations (y/n): ');
                        if($is_multiple_relation === 'y'){
                            $is_multiple_relation = true;
                        } else {
                            $is_multiple_relation = false;
                        }
                        if($is_multiple_relation){
                            $has_property_properties[] = (object) [
                                'name' => $name,
                                'type' => 'array',
                                'relation' => true,
                                'is_multiple' => true,
                            ];
                        } else {
                            $has_property_properties[] = (object) [
                                'name' => $name,
                                'type' => 'uuid',
                                'relation' => true,
                                'is_multiple' => false,
                            ];
                        }
                    }
                    elseif($has_property_type === 'dynamic'){
                        $is_multiple_dynamic = Cli::read('input', 'Are there multiple dynamics (y/n): ');
                        if($is_multiple_dynamic === 'y'){
                            $is_multiple_dynamic = true;
                        } else {
                            $is_multiple_dynamic = false;
                        }
                        if($is_multiple_dynamic){
                            $has_property_properties[] = (object) [
                                'name' => $name,
                                'type' => 'array',
                                'dynamic' => true,
                                'is_multiple' => true,
                            ];
                        } else {
                            $has_property_properties[] = (object) [
                                'name' => $name,
                                'type' => 'object',
                                'dynamic' => true,
                                'is_multiple' => false,
                            ];
                        }
                    }
                    elseif($has_property_type === 'object'){
                        $has_property_is_multiple = Cli::read('input', 'Are there multiple objects (y/n): ');
                        if($has_property_is_multiple === 'y'){
                            $has_property_is_multiple = true;
                        } else {
                            $has_property_is_multiple = false;
                        }
                        $has_property_properties[] = (object) [
                            'name' => $has_property_name,
                            'type' => $has_property_type,
                            'property' => $this->object_create_property($object, $class, ++$deep),
                            'multiple' => $has_property_is_multiple
                        ];
                    }
                    elseif($has_property_type==='microtime'){
                        $has_property_properties[] = (object) [
                            'name' => $name,
                            'type' => [
                                'float',
                                $has_property_type
                            ]
                        ];
                    }
                    elseif($has_property_type==='time'){
                        $has_property_properties[] = (object) [
                            'name' => $name,
                            'type' => [
                                'integer',
                                $has_property_type
                            ]
                        ];
                    }
                    elseif($has_property_type==='datetime'){
                        $has_property_properties[] = (object) [
                            'name' => $name,
                            'type' => [
                                'integer',
                                $has_property_type
                            ]
                        ];
                    }
                    elseif($has_property_type==='date'){
                        $has_property_properties[] = (object) [
                            'name' => $name,
                            'type' => [
                                'integer',
                                $has_property_type
                            ]
                        ];
                    } else {
                        $has_property_properties[] = (object) [
                            'name' => $has_property_name,
                            'type' => $has_property_type
                        ];
                    }
                }
                $properties[] = (object) [
                    'name' => $name,
                    'type' => $type,
                    'property' => $has_property_properties,
                    'multiple' => $is_multiple
                ];
                echo 'Object added...' . PHP_EOL;
            }
            elseif($type==='microtime'){
                $properties[] = (object) [
                    'name' => $name,
                    'type' => [
                        'float',
                        $type
                    ]
                ];
            }
            elseif($type==='time'){
                $properties[] = (object) [
                    'name' => $name,
                    'type' => [
                        'integer',
                        $type
                    ]
                ];
            }
            elseif($type==='datetime'){
                $properties[] = (object) [
                    'name' => $name,
                    'type' => [
                        'integer',
                        $type
                    ]
                ];
            }
            elseif($type==='date'){
                $properties[] = (object) [
                    'name' => $name,
                    'type' => [
                        'integer',
                        $type
                    ]
                ];
            } else {
                $properties[] = (object) [
                    'name' => $name,
                    'type' => $type
                ];
            }
        }
        return $properties;
    }
}