<?php

namespace Raxon\Org\Node\Trait\Data;

use Raxon\Org\App;
use Raxon\Org\Config;

use Raxon\Org\Module\Cli;
use Raxon\Org\Module\Controller;
use Raxon\Org\Module\Core;
use Raxon\Org\Module\Data as Storage;
use Raxon\Org\Module\Dir;
use Raxon\Org\Module\File;
use Raxon\Org\Module\Sort;

use Exception;

use Raxon\Org\Exception\FileWriteException;
use Raxon\Org\Exception\ObjectException;
use Raxon\Org\Node\Service\Security;

trait Import {

    /**
     * @throws ObjectException
     * @throws FileWriteException
     * @throws Exception
     */
    public function import($class, $role, $options=[]): false | array
    {
        Core::interactive();
        /**
         * need virtual system which manage read operations and read them on the fly
         * with an input directory and an output directory
         * then you get 2 polling scripts
         * - one polls for the input directory for new files
         * - if a file is placed in the input directory wait for it in the output directory with a time-out
         * - create an index of all unique values
         *
         */
        $name = Controller::name($class);
        $object = $this->object();
        try {
            $options = Core::object($options, Core::OBJECT_ARRAY);
            if(!array_key_exists('url', $options)){
                return false;
            }
            if(!File::exist($options['url'])){
                return false;
            }
            if(!array_key_exists('uuid', $options)){
                $options['uuid'] = false;
            }
            if(!array_key_exists('chunk-size', $options)){
                $options['chunk-size'] = 1000;
            }
            $options['import'] = true;
            set_time_limit(0);
            $object->config('raxon.org.node.import.start', microtime(true));
            $options['function'] = __FUNCTION__;
            if(!array_key_exists('relation', $options)){
                $options['relation'] = false;
            }
            $response_list = [];
            if(!Security::is_granted(
                $name,
                $role,
                $options
            )){
                return false;
            }
            $dir_data = $object->config('project.dir.node') .
                'Data' .
                $object->config('ds')
            ;
            $dir_validate = $object->config('project.dir.node') .
                'Validate' .
                $object->config('ds')
            ;
            $url = $dir_data .
                $name .
                $object->config('extension.json')
            ;
            $url_validate = $dir_validate .
                $name .
                $object->config('extension.json')
            ;
            $data_validate = $object->data_read($url_validate, sha1($url_validate));
            $this->startTransaction($name, $options);
            $data = $object->data_read($options['url']);
            if($data){
                $list = $data->data($name);
                if(!is_array($list)){
                    $list = [];
                }
                $url_object = $object->config('project.dir.node') .
                    'Object' .
                    $object->config('ds') .
                    $name .
                    $object->config('extension.json')
                ;
                $data_object = $object->data_read($url_object, sha1($url_object));
                $list_count = count($list);
                $object->config('raxon.org.node.import.list.count', $list_count);
                $list = array_chunk($list, $options['chunk-size']);
                foreach($list as $chunk_nr => $chunk) {
                    $filter_value_1 = [];
                    $filter_value_2 = [];
                    $filter_key_1 = [];
                    $filter_key_2 = [];
                    $filter_count = [];
                    $list_filter = [
                        [
                            'unique_attribute_count' => 2,
                            'list' => []
                        ],
                        [
                            'unique_attribute_count' => 1,
                            'allow_empty' => 0,
                            'list' => []
                        ],
                        [
                            'unique_attribute_count' => 1,
                            'allow_empty' => 1,
                            'list' => []
                        ]
                    ];
                    $count = 0;
                    $explode = [];
                    $create_many = [];
                    $put_many = [];
                    $patch_many = [];
                    $skip = 0;
                    $attribute = [];
                    foreach ($chunk as $record_nr => $record) {
                        $node = new Storage();
                        $node->data($record);
                        if(
                            array_key_exists('node', $options) &&
                            array_key_exists('default', $options['node'])
                        ){
                            foreach($options['node']['default'] as $attribute_default => $value){
                                $node->set($attribute_default, $value);
                            }
                            $chunk[$record_nr] = $node->data();
                        }
                        if (
                            $data_object &&
                            $data_object->has('is.unique') &&
                            !empty($data_object->get('is.unique'))
                        ) {
                            $unique = (array)$data_object->get('is.unique');
                            $unique = array_shift($unique);
                            $attribute = explode(',', $unique);
                            $count = 0;
                            foreach ($attribute as $nr => $value) {
                                $attribute[$nr] = trim($value);
                                $count++;
                            }
                            $allow_empty = $this->allow_empty($name, $data_validate, $attribute);
                            switch ($count) {
                                case 2:
                                    if (
                                        $allow_empty[0] !== false &&
                                        $allow_empty[1] !== false
                                    ) {
                                        throw new Exception('Unique value cannot be empty...');
                                    } elseif (
                                        $allow_empty[0] !== false &&
                                        $allow_empty[1] === false &&
                                        $node->has($attribute[1])
                                    ) {
                                        //1 attribute is allowed to be empty
                                        $match_1 = $node->get($attribute[0]);
                                        $match_2 = $node->get($attribute[1]);
                                        if (
                                            $match_1 !== null &&
                                            $match_1 !== '' &&
                                            $match_2 !== null &&
                                            $match_2 !== ''
                                        ) {
                                            $list_filter[0]['list'][$record_nr] = [
                                                'attribute' => [
                                                    $attribute[0],
                                                    $attribute[1]
                                                ],
                                                'value' => [
                                                    $match_1,
                                                    $match_2
                                                ]
                                            ];
                                        } elseif (
                                            $match_1 === null &&
                                            $match_2 !== null &&
                                            $match_2 !== ''
                                        ) {
                                            $list_filter[2]['allow_empty'] = $attribute[0];
                                            $list_filter[2]['list'][$record_nr] = [
                                                'attribute' => $attribute[1],
                                                'value' => $match_2,
                                            ];
                                        } else {
                                            throw new Exception('Unique value cannot be empty...');
                                        }
                                    } elseif (
                                        $allow_empty[0] === false &&
                                        $allow_empty[1] !== false &&
                                        $node->has($attribute[0])
                                    ) {
                                        //1 attribute is allowed to be empty
                                        $match_1 = $node->get($attribute[0]);
                                        $match_2 = $node->get($attribute[1]);
                                        if (
                                            $match_1 !== null &&
                                            $match_1 !== '' &&
                                            $match_2 !== null &&
                                            $match_2 !== ''
                                        ) {
                                            $list_filter[0]['list'][] = [
                                                'attribute' => [
                                                    $attribute[0],
                                                    $attribute[1]
                                                ],
                                                'value' => [
                                                    $match_1,
                                                    $match_2
                                                ]
                                            ];
                                        } elseif (
                                            $match_1 !== null &&
                                            $match_1 !== '' &&
                                            $match_2 === null
                                        ) {
                                            $list_filter[1]['allow_empty'] = $attribute[1];
                                            $list_filter[1]['list'][$record_nr] = [
                                                'attribute' => $attribute[0],
                                                'value' => $match_1,
                                            ];
                                        } else {
                                            throw new Exception('Unique value cannot be empty...');
                                        }
                                    } elseif (
                                        $allow_empty[0] === false &&
                                        $allow_empty[1] === false &&
                                        $node->has($attribute[0]) &&
                                        $node->has($attribute[1])
                                    ) {
                                        //both attributes should not be empty
                                        $match_1 = $node->get($attribute[0]);
                                        $match_2 = $node->get($attribute[1]);
                                        if (
                                            $match_1 !== null &&
                                            $match_1 !== '' &&
                                            $match_2 !== null &&
                                            $match_2 !== ''
                                        ) {
                                            $list_filter[0]['list'][$record_nr] = [
                                                'attribute' => [
                                                    $attribute[0],
                                                    $attribute[1]
                                                ],
                                                'value' => [
                                                    $match_1,
                                                    $match_2
                                                ]
                                            ];
                                        } else {
                                            throw new Exception('Unique value cannot be empty...');
                                        }
                                    } else {
                                        throw new Exception('Unique value cannot be empty...');
                                    }
                                    break;
                                case 1:
                                    if ($node->has($attribute[0])) {
                                        $match_1 = $node->get($attribute[0]);
                                        if (
                                            $match_1 !== null &&
                                            $match_1 !== ''
                                        ) {
                                            unset($list_filter[1]['allow_empty']);
                                            $list_filter[1]['list'][$record_nr] = [
                                                'attribute' => $attribute[0],
                                                'value' => $match_1,
                                            ];
                                        } else {
                                            throw new Exception('Unique value cannot be empty...');
                                        }
                                    } else {
                                        throw new Exception('Unique value cannot be empty...');
                                    }
                                    break;
                            }
                        } else {
                            $attribute[0] = 'uuid';
                            if ($node->has($attribute[0])) {
                                $match_1 = $node->get($attribute[0]);
                                if (
                                    $match_1 !== null &&
                                    $match_1 !== ''
                                ) {
                                    unset($list_filter[1]['allow_empty']);
                                    $list_filter[1]['list'][$record_nr] = [
                                        'attribute' => $attribute[0],
                                        'value' => $match_1,
                                    ];
                                } else {
                                    throw new Exception('Unique value cannot be empty...');
                                }
                            } else {
                                throw new Exception('Unique value cannot be empty...');
                            }
                        }
                    }
                    $select = $this->list(
                        $name,
                        $role,
                        [
                            'transaction' => true,
                            'limit' => '*',
                            'page' => 1
                        ]
                    );
                    $source_index = [
                        0 => [],
                        1 => [],
                        2 => []
                    ];
                    if (array_key_exists('list', $select)) {
                        foreach ($select['list'] as $nr => $record) {
                            $node = new Storage($record);
                            if(
                                array_key_exists('node', $options) &&
                                array_key_exists('default', $options['node'])
                            ){
                                foreach($options['node']['default'] as $attribute_default => $value){
                                    $node->set($attribute_default, $value);
                                }
                            }
                            if (
                                array_key_exists(0, $attribute) &&
                                array_key_exists(1, $attribute) &&
                                $node->has($attribute[0]) &&
                                $node->has($attribute[1])
                            ) {
                                $source_index[0][$node->get($attribute[0]) . ':' . $node->get($attribute[1])] = $node->get('uuid');
                            } elseif (
                                array_key_exists(0, $attribute) &&
                                $node->has($attribute[0])
                            ) {
                                $source_index[1][$node->get($attribute[0])] = $node->get('uuid');
                            } elseif (
                                array_key_exists(1, $attribute) &&
                                $node->has($attribute[1])
                            ) {
                                $source_index[2][$node->get($attribute[1])] = $node->get('uuid');
                            }
                        }
                    }
                    foreach ($list_filter as $type => $list_sub_filter) {
                        switch ($type) {
                            case 0:
                                foreach ($list_sub_filter['list'] as $record_nr => $record) {
                                    if (
                                        array_key_exists('value', $record) &&
                                        array_key_exists(0, $record['value']) &&
                                        array_key_exists(1, $record['value'])
                                    ) {
                                        $key = $record['value'][0] . ':' . $record['value'][1];
                                        if (
                                            array_key_exists($key, $source_index[0]) &&
                                            !empty($source_index[0][$key])
                                        ) {
                                            //put or patch
                                            if (
                                                array_key_exists('force', $options) &&
                                                $options['force'] === true
                                            ) {
                                                $node = new Storage($chunk[$record_nr]);
                                                $node->set('uuid', $source_index[0][$key]);
                                                $put_many[] = $node->data();
                                            } elseif (
                                                array_key_exists('patch', $options) &&
                                                $options['patch'] === true
                                            ) {
                                                $node = new Storage($chunk[$record_nr]);
                                                $node->set('uuid', $source_index[0][$key]);
                                                $patch_many[] = $node->data();
                                            } else {
                                                //skipping
                                                $skip++;
                                            }
                                        } else {
                                            //create
                                            $create_many[] = $chunk[$record_nr];
                                        }
                                    } else {
                                        //wrong type
                                        $skip++;
                                    }
                                }
                                break;
                            case 1:
                                foreach ($list_sub_filter['list'] as $record_nr => $record) {
                                    if (
                                        array_key_exists('attribute', $record) &&
                                        array_key_exists('value', $record)
                                    ) {
                                        $key = $record['value'];
                                        if (
                                            array_key_exists($key, $source_index[1]) &&
                                            !empty($source_index[1][$key])
                                        ) {
                                            //put or patch
                                            if (
                                                array_key_exists('force', $options) &&
                                                $options['force'] === true
                                            ) {
                                                $node = new Storage($chunk[$record_nr]);
                                                $node->set('uuid', $source_index[1][$key]);
                                                $put_many[] = $node->data();
                                            } elseif (
                                                array_key_exists('patch', $options) &&
                                                $options['patch'] === true
                                            ) {
                                                $node = new Storage($chunk[$record_nr]);
                                                $node->set('uuid', $source_index[1][$key]);
                                                $patch_many[] = $node->data();
                                            } else {
                                                //skipping
                                                $skip++;
                                            }
                                        } else {
                                            //create
                                            $create_many[] = $chunk[$record_nr];
                                        }
                                    } else {
                                        //wrong type
                                        $skip++;
                                    }
                                }
                                break;
                            case 2:
                                foreach ($list_sub_filter['list'] as $record_nr => $record) {
                                    if (
                                        array_key_exists('attribute', $record) &&
                                        array_key_exists('value', $record)
                                    ) {
                                        $key = $record['value'];
                                        if (
                                            array_key_exists($key, $source_index[2]) &&
                                            !empty($source_index[2][$key])
                                        ) {
                                            //put or patch
                                            if (
                                                array_key_exists('force', $options) &&
                                                $options['force'] === true
                                            ) {
                                                $node = new Storage($chunk[$record_nr]);
                                                $node->set('uuid', $source_index[2][$key]);
                                                $put_many[] = $node->data();
                                            } elseif (
                                                array_key_exists('patch', $options) &&
                                                $options['patch'] === true
                                            ) {
                                                $node = new Storage($chunk[$record_nr]);
                                                $node->set('uuid', $source_index[2][$key]);
                                                $patch_many[] = $node->data();
                                            } else {
                                                //skipping
                                                $skip++;
                                            }
                                        } else {
                                            //create
                                            $create_many[] = $chunk[$record_nr];
                                        }
                                    } else {
                                        //wrong type
                                        $skip++;
                                    }
                                }
                                break;
                        }
                    }
                    $response = $this->update(
                        $class,
                        $role,
                        $options,
                        $create_many,
                        $put_many,
                        $patch_many,
                        $skip
                    );
                    $response_list[] = $response;
                }
                if(count($response_list) === 1){
                    return $response_list[0];
                } else {
                    return $response_list;
                }
            }
        }
        catch(Exception $exception){
            $this->unlock($name);
            throw $exception;
        }
        return false;
    }

    private function allow_empty($class, $data_validate, $attribute_list=[]): array
    {
        $allow_empty = [];
        foreach($attribute_list as $nr => $attribute){
            $attribute_validate = $data_validate->get($class . '.create.validate.' . $attribute);
            if(empty($attribute_validate)){
                $allow_empty[$nr] = false;
                continue;
            }
            elseif(is_array($attribute_validate)){
                foreach($attribute_validate as $attribute_validate_nr => $attribute_validate_value){
                    if(
                        is_object($attribute_validate_value) &&
                        property_exists($attribute_validate_value, 'is.unique') &&
                        property_exists($attribute_validate_value->{'is.unique'}, 'allow_empty')
                    ){
                        $allow_empty[$nr] = $attribute_validate_value->{'is.unique'}->allow_empty;
                    } else{
                        $allow_empty[$nr] = false;
                    }
                }
            }
            else{
                $allow_empty[$nr] = false;
            }
        }
        return $allow_empty;
    }

    /**
     * @throws ObjectException
     * @throws FileWriteException
     * @throws Exception
     */
    private function update($class, $role, $options=[], $create_many=[], $put_many=[], $patch_many=[], $skip=0): array
    {
        $name = Controller::name($class);
        $object = $this->object();
        $options = Core::object($options, Core::OBJECT_ARRAY);
        $error = [];
        $dir_data = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds')
        ;
        $url = $dir_data .
            $name .
            $object->config('extension.json')
        ;
        $put = 0;
        $patch = 0;
        $create = 0;
        if(!empty($create_many)) {
            $response = $this->create_many($name, $role, $create_many, [
                'import' => true,
                'uuid' => $options['uuid'],
                'validation' => $options['validation'] ?? true,
                'function' => 'create',
                'event' => $options['event'] ?? false,
                'relation' => $options['relation'] ?? false
            ]);
            if (
                array_key_exists('list', $response) &&
                is_array($response['list'])
            ) {
                $create = count($response['list']);
            } elseif (
                array_key_exists('error', $response)
            ) {
                $error = $response['error'];
            }
        }
        if(!empty($put_many)){
            $response = $this->put_many($name, $role, $put_many, [
                'import' => true,
                'validation' => $options['validation'] ?? true,
                'function' => 'put',
                'event' => $options['event'] ?? false,
                'relation' => $options['relation'] ?? false
            ]);
            if(
                array_key_exists('list', $response) &&
                is_array($response['list'])
            ) {
                $put = count($response['list']);
            }
            elseif(
                array_key_exists('error', $response)
            ){
                $error = array_merge($error, $response['error']);
            }
        }
        if(!empty($patch_many)){
            $response = $this->patch_many($name, $role, $patch_many, [
                'import' => true,
                'validation' => $options['validation'] ?? true,
                'function' => 'patch',
                'event' => $options['event'] ?? false,
                'relation' => $options['relation'] ?? false
            ]);
            if(
                array_key_exists('list', $response) &&
                is_array($response['list'])
            ) {
                $patch = count($response['list']);
            }
            elseif(
                array_key_exists('error', $response)
            ){
                $error = array_merge($error, $response['error']);
            }
        }
        if(!empty($error)){
            $this->unlock($name);
            $object->config('delete', 'node.transaction.' . $name);
            return [
                'error' => $error,
                'transaction' => true,
                'duration' => (microtime(true) - $object->config('raxon.org.node.import.start')) * 1000
            ];
        }
        $commit = [];
        if($create > 0 || $put > 0 || $patch > 0){
            $object->config('time.limit', 0);
            $commit = $this->commit($class, $role);
        } else {
            $this->unlock($name);
        }
        $duration = microtime(true) - $object->config('raxon.org.node.import.start');
        $total = $put + $patch + $create;
        $item_per_second = round($total / $duration, 2);
        return [
            'skip' => $skip,
            'put' => $put,
            'patch' => $patch,
            'create' => $create,
            'commit' => $commit,
            'mtime' => File::mtime($url),
            'duration' => $duration * 1000,
            'item_per_second' => $item_per_second,
            'transaction' => true
        ];
    }
}