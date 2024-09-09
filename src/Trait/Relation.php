<?php
namespace Raxon\Node\Trait;

use Raxon\Module\Core;
use Raxon\Module\Controller;
use Raxon\Module\Data as Storage;
use Raxon\Module\File;
use Raxon\Module\Filter as Module;
use Raxon\Module\Parse;
use Raxon\Module\Route;

use Exception;

use Raxon\Exception\FileWriteException;
use Raxon\Exception\ObjectException;

trait Relation {

    /**
     * @throws Exception
     */
    private function relation($record, $data, $role, $options=[]): array | object
    {
        $object = $this->object();
        if(!$role){
            return $record;
        }
        if($data){
            $node = new Storage($record);
            $relations = $data->data('relation');
            if(!$relations){
                return $record;
            }
            if(
                array_key_exists('relation', $options) &&
                is_bool($options['relation']) &&
                $options['relation'] === false
            ){
                return $record;
            }
            if(!is_array($relations)){
                return $record;
            }
            foreach($relations as $relation){
                if(
                    is_object($relation) &&
                    property_exists($relation, 'type') &&
                    property_exists($relation, 'class') &&
                    property_exists($relation, 'attribute')
                ){
                    $is_allowed = false;
                    $output_filter = false;
                    $options_relation = $options['relation'] ?? [];
                    if(is_bool($options_relation) && $options_relation === true){
                        $is_allowed = true;
                    }
                    elseif(is_bool($options_relation) && $options_relation === false){
                        $is_allowed = false;
                    }
                    elseif(is_array($options_relation)){
                        foreach($options_relation as $option){
                            if(strtolower($option) === strtolower($relation->class)){
                                $is_allowed = true;
                                break;
                            }
                        }
                    }
                    if(
                        property_exists($relation, 'output') &&
                        !empty($relation->output) &&
                        is_object($relation->output) &&
                        property_exists($relation->output, 'filter') &&
                        !empty($relation->output->filter) &&
                        is_array($relation->output->filter)
                    ){
                        $output_filter = $relation->output->filter;
                    }
                    switch(strtolower($relation->type)){
                        case 'one-one':
                            if(
                                $is_allowed &&
                                $node->has($relation->attribute)
                            ){
                                $uuid = $node->get($relation->attribute);
                                if(!is_string($uuid)){
                                    break;
                                }
                                if($uuid === '*'){
                                    $one_one = [
                                        'sort' => [
                                            'uuid' => 'ASC'
                                        ],
                                        'relation' => $options['relation'],
                                        'ramdisk' => $options['ramdisk'] ?? false,
                                        'ramdisk_dir' => $options['ramdisk_dir'] ?? false
                                    ];
                                    $response = $this->record(
                                        $relation->class,
                                        $this->role_system(),
                                        $one_one
                                    );
                                    if(
                                        !empty($response) &&
                                        array_key_exists('node', $response)
                                    ){
                                        $output_filter_options = $options;
                                        if(property_exists($relation, 'output')){
                                            $output_filter_options['output'] = [];
                                            $output_filter_options['output']['filter'] = $output_filter;
                                        }
                                        $response['list'] = $this->nodelist_output_filter($object,  [ $response['node'] ], $output_filter_options);
                                        $node->set($relation->attribute, $response['list'][0]);
                                    } else {
                                        $node->set($relation->attribute, false);
                                    }
                                } else {
                                    $response = $this->read(
                                        $relation->class,
                                        $this->role_system(),
                                        [
                                            'uuid' => $uuid,
                                            'relation' => $options['relation'],
                                            'ramdisk' => $options['ramdisk'] ?? false,
                                            'ramdisk_dir' => $options['ramdisk_dir'] ?? false
                                        ]
                                    );
                                    if (
                                        array_key_exists('node', $response) &&
                                        property_exists($response['node'], 'uuid')
                                    ){
                                        $output_filter_options = $options;
                                        if(property_exists($relation, 'output')){
                                            $output_filter_options['output'] = [];
                                            $output_filter_options['output']['filter'] = $output_filter;
                                        }
                                        $response['list'] = $this->nodelist_output_filter($object,  [ $response['node'] ], $output_filter_options);
                                        $node->set($relation->attribute, $response['list'][0]);
                                    }
                                }
                            }
                            $record = $node->data();
                            break;
                        case 'one-many':
                            if(
                                $is_allowed &&
                                $node->has($relation->attribute)
                            ){
                                $one_many = $node->get($relation->attribute);
                                if(is_object($one_many)){
                                    if(!property_exists($one_many, 'limit')){
                                        throw new Exception('Relation: ' . $relation->attribute . ' has no limit');
                                    }
                                    if(!property_exists($one_many, 'page')){
                                        $one_many->page = 1;
                                    }
                                    if($one_many->limit === '*'){
                                        $one_many->page = 1;
                                    }
                                    if(!property_exists($one_many, 'sort')){
                                        if(property_exists($relation, 'sort')){
                                            $one_many->sort = $relation->sort;
                                        } else {
                                            $one_many->sort = [
                                                'uuid' => 'ASC'
                                            ];
                                        }
                                    }
                                    if(
                                        property_exists($one_many, 'output') &&
                                        !empty($one_many->output) &&
                                        is_object($one_many->output) &&
                                        property_exists($one_many->output, 'filter') &&
                                        !empty($one_many->output->filter) &&
                                        is_array($one_many->output->filter)
                                    ){
                                        $output_filter = $one_many->output->filter;
                                    }
                                    $one_many->relation = $options['relation'] ?? [];
                                    $one_many->ramdisk = $options['ramdisk'] ?? false;
                                    $one_many->ramdisk_dir = $options['ramdisk_dir'] ?? false;
                                    $response = $this->list(
                                        $relation->class,
                                        $this->role_system(),
                                        $one_many
                                    );
                                    if(
                                        !empty($response) &&
                                        array_key_exists('list', $response)
                                    ){
                                        $output_filter_options = $options;
                                        if(property_exists($relation, 'output')){
                                            $output_filter_options['output'] = [];
                                            $output_filter_options['output']['filter'] = $output_filter;
                                        }
                                        $response['list'] = $this->nodelist_output_filter($object, $response['list'], $output_filter_options);
                                        $node->set($relation->attribute, $response['list']);
                                    } else {
                                        $node->set($relation->attribute, []);
                                    }
                                    $record = $node->data();
                                    break;
                                }
                                elseif($one_many === '*'){
                                    $one_many = (object) [
                                        'limit' => '*',
                                        'page' => 1,
                                        'ramdisk' => $options['ramdisk'] ?? false,
                                        'ramdisk_dir' => $options['ramdisk_dir'] ?? false,
                                        'relation' => $options['relation'] ?? []
                                    ];
                                    if(property_exists($relation, 'sort')){
                                        $one_many->sort = $relation->sort;
                                    } else {
                                        $one_many->sort = [
                                            'uuid' => 'ASC'
                                        ];
                                    }
                                    $response = $this->list(
                                        $relation->class,
                                        $this->role_system(),
                                        $one_many
                                    );
                                    if(
                                        !empty($response) &&
                                        array_key_exists('list', $response)
                                    ){
                                        $output_filter_options = $options;
                                        if(property_exists($relation, 'output')){
                                            $output_filter_options['output'] = [];
                                            $output_filter_options['output']['filter'] = $output_filter;
                                        }
                                        $response['list'] = $this->nodelist_output_filter($object, $response['list'], $output_filter_options);
                                        $node->set($relation->attribute, $response['list']);
                                    } else {
                                        $node->set($relation->attribute, []);
                                    }
                                    $record = $node->data();
                                    break;
                                }
                                elseif($one_many === 'relation'){
                                    if(
                                        property_exists($relation, 'limit') &&
                                        !empty($relation->limit) &&
                                        (
                                            $relation->limit === '*' ||
                                            (
                                                is_int($relation->limit) ||
                                                is_float($relation->limit)
                                            )
                                        )
                                    ){
                                        if(
                                            property_exists($relation, 'page') &&
                                            (
                                                is_int($relation->page) ||
                                                is_float($relation->page)
                                            )
                                        ){
                                            $page = $relation->page;
                                        } else {
                                            $page = 1;
                                        }
                                        $one_many = (object) [
                                            'limit' => $relation->limit,
                                            'page' => $page,
                                            'ramdisk' => $options['ramdisk'] ?? false,
                                            'ramdisk_dir' => $options['ramdisk_dir'] ?? false,
                                            'relation' => $options['relation'] ?? []
                                        ];
                                        if($one_many->limit === '*'){
                                            $one_many->page = 1;
                                        }
                                        if(
                                            property_exists($relation, 'sort') &&
                                            !empty($relation->sort)
                                        ){
                                            $one_many->sort = $relation->sort;
                                        } else {
                                            $one_many->sort = [
                                                'uuid' => 'ASC'
                                            ];
                                        }
                                        if(
                                            property_exists($relation, 'where') &&
                                            !empty($relation->where)
                                        ){
                                            $one_many->where = $relation->where;
                                        }
                                        if(
                                            property_exists($relation, 'filter') &&
                                            !empty($relation->filter) &&
                                            is_array($relation->filter)
                                        ){
                                            $one_many->filter = $relation->filter;
                                        }
                                        $response = $this->list(
                                            $relation->class,
                                            $this->role_system(),
                                            $one_many
                                        );
                                        if(
                                            !empty($response) &&
                                            array_key_exists('list', $response)
                                        ){
                                            $output_filter_options = $options;
                                            if(property_exists($relation, 'output')){
                                                $output_filter_options['output'] = $output_filter;
                                            }
                                            $response['list'] = $this->nodelist_output_filter($object, $response['list'], $output_filter_options);
                                            $node->set($relation->attribute, $response['list']);
                                        } else {
                                            $node->set($relation->attribute, []);
                                        }
                                        $record = $node->data();
                                        break;
                                    }
                                }
                                if(!is_array($one_many)){
                                    break;
                                }
                                $options_one_many = (object) [
                                    'limit' => '*',
                                    'page' => 1,
                                    'sort' => [
                                        'uuid' => 'ASC'
                                    ],
                                    'relation' => $options['relation'],
                                    'ramdisk' => $options['ramdisk'] ?? false,
                                    'ramdisk_dir' => $options['ramdisk_dir'] ?? false
                                ];
                                $response = $this->list(
                                    $relation->class,
                                    $this->role_system(),
                                    $options_one_many
                                );
                                if(
                                    !empty($response) &&
                                    array_key_exists('list', $response)
                                ){
                                    $output_filter_options = $options;
                                    if(property_exists($relation, 'output')){
                                        $output_filter_options['output'] = Core::object($relation->output, Core::OBJECT_ARRAY);
                                    }
                                    $response['list'] = $this->nodelist_output_filter($object, $response['list'], $output_filter_options);
                                    $index = new Storage();
                                    foreach($response['list'] as $nr => $record){
                                        if(
                                            is_object($record) &&
                                            property_exists($record, 'uuid')
                                        ){
                                            $index->set($record->uuid, $record);
                                        }
                                    }
                                    foreach($one_many as $nr => $uuid){
                                        if(!is_string($uuid)){
                                            continue;
                                        }
                                        if($index->has($uuid)){
                                            $one_many[$nr] = $index->get($uuid);
                                        } else {
                                            unset($one_many[$nr]);
                                        }
                                    }
                                    $node->set($relation->attribute, $one_many);
                                } else {
                                    $node->set($relation->attribute, []);
                                }
                            }
                            $record = $node->data();
                            break;
                        case 'many-one':
                            if(
                                $is_allowed &&
                                $node->has($relation->attribute)
                                //add is_uuid
                            ){
                                $uuid = $node->get($relation->attribute);
                                if(!is_string($uuid)){
                                    break;
                                }
                                throw new Exception('Not implemented yet...');
                                /*
                                $relation_url = $object->config('project.dir.data') .
                                    'Node' .
                                    $object->config('ds') .
                                    'Storage' .
                                    $object->config('ds') .
                                    substr($uuid, 0, 2) .
                                    $object->config('ds') .
                                    $uuid .
                                    $object->config('extension.json')
                                ;
                                $relation_data = $object->data_read($relation_url, sha1($relation_url));
                                if($relation_data){
                                    if(
                                        $relation_data->has('#class')
                                    ) {
                                        $relation_object_url = $object->config('project.dir.data') .
                                            'Node' .
                                            $object->config('ds') .
                                            'Object' .
                                            $object->config('ds') .
                                            $relation_data->get('#class') .
                                            $object->config('extension.json')
                                        ;
                                        $relation_object_data = $object->data_read($relation_object_url, sha1($relation_object_url));
                                        if($relation_object_data){
                                            foreach($relation_object_data->get('relation') as $relation_nr => $relation_relation){
                                                if(
                                                    property_exists($relation_relation, 'type') &&
                                                    property_exists($relation_relation, 'class') &&
                                                    property_exists($record, '#class') &&
                                                    $relation_relation->type === 'many-one' &&
                                                    $relation_relation->class === $record->{'#class'}
                                                ){
                                                    //don't need cross-reference, parent is this.
                                                    continue;
                                                }
                                                elseif(
                                                    property_exists($relation_relation, 'type') &&
                                                    property_exists($relation_relation, 'class') &&
                                                    property_exists($record, '#class') &&
                                                    $relation_relation->type === 'one-one' &&
                                                    $relation_relation->class === $record->{'#class'}
                                                ){
                                                    //don't need cross-reference, parent is this.
                                                    continue;
                                                }
                                                elseif(
                                                    property_exists($relation_relation, 'type') &&
                                                    property_exists($relation_relation, 'class') &&
                                                    property_exists($record, '#class') &&
                                                    $relation_relation->type === 'one-many' &&
                                                    $relation_relation->class === $record->{'#class'}
                                                ){
                                                    //don't need cross-reference, parent is this.
                                                    continue;
                                                }
                                                if(
                                                    property_exists($relation_relation, 'attribute')
                                                ){
                                                    $relation_data_data = $relation_data->get($relation_relation->attribute);
                                                    $relation_data_data = $this->relation_inner($relation_relation, $relation_data_data, $options);
                                                    $relation_data->set($relation_relation->attribute, $relation_data_data);
                                                }
                                            }
                                        }
                                        if($relation_data){
                                            $node->set($relation->attribute, $relation_data->data());
                                        }
                                    }
                                }
                                */
                            }
                            $record = $node->data();
                            break;
                    }
                    /* done already in raxon/node nodelist 1575
                    if(
                        empty($output_filter) &&
                        property_exists($relation, 'output') &&
                        !empty($relation->output) &&
                        is_object($relation->output) &&
                        property_exists($relation->output, 'filter') &&
                        !empty($relation->output->filter) &&
                        is_array($relation->output->filter)
                    ){
                        $output_filter = $relation->output->filter;
                    }
                    if($output_filter){
                        foreach($output_filter as $output_filter_nr => $output_filter_data){
                            $route = (object) [
                                'controller' => $output_filter_data
                            ];
                            $route = Route::controller($route);
                            if(
                                property_exists($route, 'controller') &&
                                property_exists($route, 'function') &&
                                property_exists($record, $relation->attribute)
                            ){
                                $record->{$relation->attribute} = $route->controller::{$route->function}($object, $record->{$relation->attribute});
                            }
                        }
                    }
                    */
                }
            }
        }
        return $record;
    }

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function relation_mtime($object_data): array
    {
        $mtime = [];
        if(empty($object_data)) {
            return [];
        }
        $object = $this->object();
        $relations = $object_data->get('relation');
        $loaded = $object->config('raxon.org.node.relation.mtime.loaded');
        $is_outer = false;
        if(!$loaded){
            $is_outer = true;
            $loaded = [];
        }
        if(
            !empty($relations) &&
            is_array($relations)
        ){
            foreach($relations as $relation){
                if(!property_exists($relation, 'class')){
                    continue;
                }
                if(in_array($relation->class, $loaded, true)){
                    continue;
                }
                $loaded[] = $relation->class;
                $object->config('raxon.org.node.relation.mtime.loaded', $loaded);
                $object_url = $object->config('project.dir.node') .
                    'Object' .
                    $object->config('ds') .
                    $relation->class .
                    $object->config('extension.json')
                ;
                $data_url = $object->config('project.dir.node') .
                    'Data' .
                    $object->config('ds') .
                    $relation->class .
                    $object->config('extension.json')
                ;
                $data = $object->data_read($object_url, sha1($object_url));
                if(!$data){
                    continue;
                }
                $mtime[$data_url] = File::mtime($data_url);
                $data_mtime = $this->relation_mtime($data);
                foreach($data_mtime as $url => $value){
                    $mtime[$url] = $value;
                }
            }
        }
        $object->config('raxon.org.node.relation.mtime.loaded', $loaded);
        if($is_outer){
            $object->config('delete', 'raxon.org.node.relation.mtime.loaded');
        }
        return $mtime;
    }
}