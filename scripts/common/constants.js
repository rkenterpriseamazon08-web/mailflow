const MASTER_HEADERS = [
  'record_id','name','email_id','company_name','custom_line','business_segment','status','campaign_name','template_name','sent_at','error_message','source_file_name','uploaded_at','updated_at',
  ...Array.from({ length: 10 }, (_, i) => [
    `follow_up_${i + 1}_status`,
    `follow_up_${i + 1}_sent_at`,
    `follow_up_${i + 1}_template_name`,
    `follow_up_${i + 1}_error_message`
  ]).flat()
];

const FIELD_ALIASES = {
  name: ['name', 'full name', 'contact name'],
  email_id: ['email id', 'email', 'e-mail', 'email address', 'mail'],
  company_name: ['company name', 'company', 'organization', 'org'],
  custom_line: ['custom line', 'custom', 'custom message'],
  business_segment: ['business segment', 'segment', 'industry', 'vertical'],
  status: ['status', 'send status']
};

module.exports = { MASTER_HEADERS, FIELD_ALIASES };
