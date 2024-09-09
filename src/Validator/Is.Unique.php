<?php

/**
 * @author          Remco van der Velde
 * @since           2020-09-18
 * @copyright       Remco van der Velde
 * @license         MIT
 * @version         1.0
 * @changeLog
 *     -            all
 */


use Raxon\Org\App;

use Raxon\Org\Module\Controller;
use Raxon\Org\Module\Dir;
use Raxon\Org\Module\File;
use Raxon\Org\Module\Filter;
use Raxon\Org\Module\Data as Storage;
use Raxon\Org\Module\Parse;

use Raxon\Org\Node\Model\Node;

/**
 * @throws Exception
 */
function validate_is_unique(App $object, $value='', $attribute='', $validate='', $function=false): bool
{
    $dir_node = $object->config('project.dir.node');
    $url = false;
    $name = false;
    $allow_empty = false;
    $uuid = $object->request('node.uuid');
    if (is_object($validate)) {
        if (property_exists($validate, 'class')) {
            $name = Controller::name($validate->class);
            $url = $dir_node . 'Data' . $object->config('ds') . $name . $object->config('extension.json');
        }
        if (property_exists($validate, 'attribute')) {
            $attribute = $validate->attribute;
            $value = [];
            $explode = [];
            $value_count = 0;
            if (is_array($attribute)) {
                foreach ($attribute as $nr => $record) {
                    $explode = explode(':', $record);
                    foreach($explode as $explode_nr => $explode_value){
                        $explode[$explode_nr] = trim($explode_value);
                    }
                    $value[$nr] = $object->request('node.' . trim($explode[0]));
                    $value_count++;
                }
            }
            if(
                $value_count === 1 &&
                $value[0] === null ||
                $value[0] === '' &&
                array_key_exists(0, $explode)
            ){
                $logger_node = $object->logger('node');
                if($logger_node){
                    $logger_node->info('Is.Unique: ' . $explode[0] . ' is empty', [ $uuid ]);
                }
                return false;
            }
        }
        if(property_exists($validate, 'allow_empty')) {
            $allow_empty = explode(':', $validate->allow_empty);
        }
    }
    if(empty($function)){
        $function = 'record';
    }
    if (
        is_array($attribute) &&
        is_array($value)
    ) {
        $options = [
            'filter' => [],
            'function' => $function
        ];
        foreach ($attribute as $nr => $record) {
            if (array_key_exists($nr, $value)) {
                $explode = explode(':', $record);
                foreach($explode as $explode_nr => $explode_value){
                    $explode[$explode_nr] = trim($explode_value);
                }
                if($allow_empty === false){
                    if(array_key_exists(1, $explode)){
                        $options['filter'][$explode[1]]['operator'] = Filter::OPERATOR_STRICTLY_EXACT;
                        $options['filter'][$explode[1]]['value'] = $value[$nr];
                    } else {
                        $options['filter'][$explode[0]]['operator'] = Filter::OPERATOR_STRICTLY_EXACT;
                        $options['filter'][$explode[0]]['value'] = $value[$nr];
                    }
                } else {
                    if(
                        array_key_exists(1, $allow_empty) &&
                        array_key_exists(1, $explode) &&
                        $allow_empty[1] === $explode[1] &&
                        (
                            $value[$nr] === null ||
                            $value[$nr] === ''
                        )
                    ){
                        continue;
                    }
                    elseif(
                        array_key_exists(0, $allow_empty) &&
                        array_key_exists(0, $explode) &&
                        $allow_empty[0] === $explode[0] &&
                        (
                            $value[$nr] === null ||
                            $value[$nr] === ''
                        )
                    ){
                        continue;
                    }
                    elseif(
                        array_key_exists(1, $explode)
                    ){
                        $options['filter'][$explode[1]]['operator'] = Filter::OPERATOR_STRICTLY_EXACT;
                        $options['filter'][$explode[1]]['value'] = $value[$nr];
                    } else {
                        $options['filter'][$explode[0]]['operator'] = Filter::OPERATOR_STRICTLY_EXACT;
                        $options['filter'][$explode[0]]['value'] = $value[$nr];
                    }
                }
            }
        }
    } else {
        $options = [
            'filter' => [
                $attribute => [
                    'operator' => Filter::OPERATOR_STRICTLY_EXACT,
                    'value' => $value
                ]
            ],
            'function' => $function
        ];
    }
//    $options['memory'] = true;
    $node = new Node($object);
    if(str_ends_with($options['function'], '_many')){
        //create index
//        $options['index'] = $node->index($name, $node->role_system(), $options);
        //add index as options to record
    }
    $response = $node->record($name, $node->role_system(), $options);
    if(
        !empty($response) &&
        is_array($response) &&
        array_key_exists('node', $response)
    ){
        $record = $response['node'];
        if(
            is_object($record) &&
            property_exists($record, 'uuid') &&
            !empty($record->uuid)
        ){
            if($uuid === $record->uuid){
                //can patch, can put
                return true;
            }
            return false;
        } else {
            return true;
        }
    } else {
        return true;
    }
}
